package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
}

type LastApply struct {
	commandId    int
	commandReply *CommandReply
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
	term int
}

func (kv *KVServer) newWaitChan(index int) chan *CommandReply {
	kv.waitChan[index] = make(chan *CommandReply)
	return kv.waitChan[index]
}
func (kv *KVServer) delWaitChanUL(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//close(kv.waitChan[index])
	//delete(kv.waitChan, index)
	kv.waitChan = make(map[int]chan *CommandReply)
}

//判断是否是重复命令
func (kv *KVServer) DuplicationCommand(clientId int64, commandId int) (*CommandReply, bool) {
	if lastApply, ok := kv.commandApplyTable[clientId]; ok && lastApply.commandId == commandId {
		return lastApply.commandReply, true
	}
	return nil, false
}
func (kv *KVServer) ReceiveCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if reply1, ok := kv.DuplicationCommand(args.ClientId, args.CommandId); ok && args.Op != GET {
		reply.Value, reply.Err = reply1.Value, reply1.Err
		DPrintf("[%v]client--DuplicationCommand--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
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
	index, kv.term, isLeader = kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		DPrintf("[%v]client--NotLeader--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
		kv.mu.Unlock()
		return
	}
	ch := kv.newWaitChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("[%v]client--SuccessCommand--:from [%v] commandId-%v,%v-Key-%v-value-%v-resultValue-%v", kv.me, args.ClientId, args.CommandId, args.Op, args.Key, args.Value, reply.Value)
	case <-time.After(kv.timeout):
		reply.Err = Timeout
		DPrintf("[%v]client--Timeout--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
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

					//因可能多次提交,重复的commit就不执行了
					if _, ok := kv.DuplicationCommand(op.ClientId, op.CommandId); ok && op.Opt != GET {
						kv.mu.Unlock()
					} else {
						kv.mu.Unlock()
						//应用到数据库
						if op.Opt == GET {
							reply.Value, reply.Err = kv.database.Get(op.Key)
						} else if op.Opt == PUT {
							reply.Err = kv.database.Put(op.Key, op.Value)
						} else {
							reply.Err = kv.database.Append(op.Key, op.Value)
						}

						kv.mu.Lock()
						//不是leader不提交结果
						currentTerm, isLeader := kv.rf.GetState()
						//保存上一次执行结果以及通知返回结果
						if op.Opt != GET {
							kv.commandApplyTable[op.ClientId] = &LastApply{
								commandId:    op.CommandId,
								commandReply: reply,
							}
						}

						//仅在我等结果,并且我是Leader,term没有改变的情况下才返回,否则index被覆盖了导致错误
						if ch, ok := kv.waitChan[applyCommand.CommandIndex]; ok && isLeader && currentTerm == kv.term {
							ch <- reply
						}
						kv.mu.Unlock()
					}
				}
			}

		}
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
	kv.database = DataBase{table: make(map[string]string, 10)}
	kv.waitChan = make(map[int]chan *CommandReply, 10)
	kv.commandApplyTable = make(map[int64]*LastApply, 10)
	kv.timeout = time.Duration(100) * time.Millisecond

	// You may need initialization code here.

	go kv.listenApply()
	return kv
}
