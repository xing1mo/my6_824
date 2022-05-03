package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Opt       Opt
	ClientId  int64
	CommandId int

	//标识该命令生成时的term,若后续的Term发生变化,表示重新选举了,旧的命令不执行
	//server失去leader地位或者Term改变后，抛弃apply结果，因为可能原index被覆盖了，导致出现错误结果
	//不能标识在这里,因为提交操作时拿不到Term,会导致多次加锁出现Term不对,修改RAFT记录Term
	//Term int
}

type LastApply struct {
	CommandId    int
	CommandReply *CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//保存数据库
	database DataBase
	//接受某个index对应的消息
	waitChan map[int]chan *CommandReply
	//保存上一次的结果
	commandApplyTable map[int64]*LastApply
	//超时重发时间
	timeout time.Duration
	//提交该命令时的Term
	//Lab4因不只有client的请求生成Log了,因此每条Log都需要标记Term,修改RAFT记录
	//term int
	//上次执行命令的位置
	lastApplyIndex int
	//上次snapshot的位置
	//lastSnapshotIndex int
}

func (kv *KVServer) newWaitChan(index int) chan *CommandReply {
	//3.一开始在Lab3中使用chan监测是否有Log提交，从而触发数据返回给客户端。
	//在对chan进行删除时使用的方法是chan直接清空，这里在Lab3中因为只有client中的请求会生成chan所以没有问题。
	//但到了Lab4有了config的更改，需要删除对应index处的chan，这时会出现卡死的情况，
	//当命令超时重发时间延长到500ms可以解决，但并不知道原因。后发现存在一种情况，
	//即一个Log在Raft层成功提交了，但没有及时被kv数据库接收到，导致超时重发，
	//但超时删除chan的时候需要锁，没有及时将chan删除，这时Log提交被检测到然后抢锁了，
	//将数据写入本该删除的chan，而此时chan的接收端已被超时触发，导致阻塞,将chan改为非阻塞的。
	kv.waitChan[index] = make(chan *CommandReply, 2)
	return kv.waitChan[index]
}
func (kv *KVServer) delWaitChanUL(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.waitChan[index])
	delete(kv.waitChan, index)

	//Lab4不能统一删除
	//kv.waitChan = make(map[int]chan *CommandReply)
}

//判断是否是重复命令
func (kv *KVServer) DuplicationCommand(clientId int64, commandId int) (*CommandReply, bool) {
	if lastApply, ok := kv.commandApplyTable[clientId]; ok && lastApply.CommandId == commandId {
		return lastApply.CommandReply, true
	}
	return nil, false
}
func (kv *KVServer) ReceiveCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if reply1, ok := kv.DuplicationCommand(args.ClientId, args.CommandId); ok && args.Op != GET {
		reply.Value, reply.Err = reply1.Value, reply1.Err
		//DPrintf("[%v]client--DuplicationCommand--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
		kv.mu.Unlock()
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Opt:       args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	var index int
	var isLeader bool
	//op.Term, _ = kv.rf.GetState()
	index, _, isLeader = kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		//DPrintf("[%v]client--NotLeader--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
		kv.mu.Unlock()
		return
	}
	ch := kv.newWaitChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		//DPrintf("[%v]client--SuccessCommand--:from [%v] commandId-%v,%v-Key-%v-value-%v-resultValue-%v", kv.me, args.ClientId, args.CommandId, args.Op, args.Key, args.Value, reply.Value)
	case <-time.After(kv.timeout):
		reply.Err = Timeout
		//DPrintf("[%v]client--Timeout--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
	}
	go kv.delWaitChanUL(index)
}

//监听是否有Command apply了
func (kv *KVServer) listenApply() {
	for !kv.killed() {
		select {
		case applyCommand := <-kv.applyCh:
			{
				reply := new(CommandReply)
				if applyCommand.CommandValid {
					op := applyCommand.Command.(Op)
					kv.mu.Lock()
					if applyCommand.CommandIndex <= kv.lastApplyIndex {
						//小概率出现snapshot先提交进chan但未执行,重复在snapshot里的command同时提交的情况,导致命令重复执行，需在命令执行处特判。
						kv.mu.Unlock()
					} else {
						//因可能多次提交,重复的commit就不执行了
						if _, ok := kv.DuplicationCommand(op.ClientId, op.CommandId); ok && op.Opt != GET {
							kv.mu.Unlock()
						} else {
							//应用到数据库
							//对数据库操作要上锁,防止state改了但applyIndex没改,快照出错
							if op.Opt == GET {
								reply.Value, reply.Err = kv.database.Get(op.Key)
							} else if op.Opt == PUT {
								reply.Err = kv.database.Put(op.Key, op.Value)
							} else {
								reply.Err = kv.database.Append(op.Key, op.Value)
							}

							kv.lastApplyIndex = applyCommand.CommandIndex
							//不是leader不提交结果
							currentTerm, isLeader := kv.rf.GetState()
							//保存上一次执行结果以及通知返回结果
							if op.Opt != GET {
								kv.commandApplyTable[op.ClientId] = &LastApply{
									CommandId:    op.CommandId,
									CommandReply: reply,
								}
							}
							//仅在我等结果,并且我是Leader,term没有改变的情况下才返回,否则index被覆盖了导致错误
							if ch, ok := kv.waitChan[applyCommand.CommandIndex]; ok && isLeader && currentTerm == applyCommand.CommandTerm {
								ch <- reply
							}
							kv.mu.Unlock()
						}
					}
				} else if applyCommand.SnapshotValid && kv.rf.CondInstallSnapshot(applyCommand.SnapshotTerm, applyCommand.SnapshotIndex, applyCommand.Snapshot) {
					kv.mu.Lock()
					kv.lastApplyIndex = applyCommand.SnapshotIndex
					kv.setSnapshot(applyCommand.Snapshot)
					kv.mu.Unlock()
				}
			}

		}
	}
}

//轮询log的大小看是否需要Snapshot
func (kv *KVServer) listenSnapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.rf.GetPersist().RaftStateSize() >= kv.maxraftstate/4*3 {
			kv.rf.Snapshot(kv.lastApplyIndex, kv.getSnapshotUL())
			for s, s2 := range kv.database.Table {
				DPrintf("listenSnapshot:%v--%v\n\n", s, s2)
			}
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

//获得当前状态机的snapshot
func (kv *KVServer) getSnapshotUL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.database.Mu.Lock()
	e.Encode(kv.database.Table)
	kv.database.Mu.Unlock()

	//e.Encode(kv.waitChan)
	e.Encode(kv.commandApplyTable)
	//e.Encode(kv.term)
	//e.Encode(kv.lastSnapshotIndex)
	data := w.Bytes()
	return data
}

//重启读取snapshot时初始化状态机
func (kv *KVServer) setSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Table map[string]string
	//var waitChan map[int]chan *CommandReply
	var commandApplyTable map[int64]*LastApply
	//var lastSnapshotIndex int
	if err := d.Decode(&Table); err != nil {
		DPrintf("[%v][read Table error]-%v", kv.me, err)
	} else if err := d.Decode(&commandApplyTable); err != nil {
		DPrintf("[%v][read commandApplyTable error]-%v", kv.me, err)
	} else {
		kv.database.Table = Table
		//kv.waitChan = waitChan
		kv.commandApplyTable = commandApplyTable
		//kv.term = term
		//kv.lastSnapshotIndex = lastSnapshotIndex
		//kv.mu.Lock()
		//for s, s2 := range kv.database.Table {
		//	DPrintf("listenSnapshot:%v--%v\n\n", s, s2)
		//}
		//kv.mu.Unlock()
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.database = DataBase{Table: make(map[string]string, 10)}
	kv.waitChan = make(map[int]chan *CommandReply, 10)
	kv.commandApplyTable = make(map[int64]*LastApply, 10)
	kv.timeout = time.Duration(500) * time.Millisecond
	kv.lastApplyIndex = 0
	//kv.lastSnapshotIndex = 0

	//读取snapshot
	kv.setSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.

	if maxraftstate != -1 {
		go kv.listenSnapshot()
	}
	go kv.listenApply()
	return kv
}
