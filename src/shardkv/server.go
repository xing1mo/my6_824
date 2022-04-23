package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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
	//Term int

	Shard     int
	ConfigNum int
	Data      map[string]string
}

type LastApply struct {
	CommandId    int
	CommandReply *CommandReply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dead int32 // set by Kill()

	//controller client
	mck *shardctrler.Clerk

	//保存数据库
	database DataBase
	//接受某个index对应的消息
	waitChan map[int]chan *CommandReply
	//保存上一次的结果
	commandApplyTable map[int64]*LastApply
	//超时重发时间
	timeout time.Duration
	//提交该命令时的Term
	//term int
	//上次执行命令的位置
	lastApplyIndex int

	//下面是config change所需要的变量
	//当前group所处于的状态,防止重复更新config生成重复pull,push池,不持久化
	//为防止未生成pull和push池便开始下一轮的configNum增加,state用来标识当前改变的config版本,未改变用-1表示
	state int
	//pull池拉取数据
	pullPool map[int]bool
	//push池发出数据
	pushPool map[int]bool

	//所有shard信息
	isServeShard [shardctrler.NShards]bool
	//当前server使用的config
	config shardctrler.Config
}

func (kv *ShardKV) newWaitChan(index int) chan *CommandReply {
	kv.waitChan[index] = make(chan *CommandReply)
	return kv.waitChan[index]
}
func (kv *ShardKV) delWaitChanUL(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//close(kv.waitChan[index])
	delete(kv.waitChan, index)
	//kv.waitChan = make(map[int]chan *CommandReply)
}

//判断是否是重复命令
func (kv *ShardKV) DuplicationCommand(clientId int64, commandId int) (*CommandReply, bool) {
	if lastApply, ok := kv.commandApplyTable[clientId]; ok && lastApply.CommandId == commandId {
		return lastApply.CommandReply, true
	}
	return nil, false
}
func (kv *ShardKV) ReceiveCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()

	//不是我服务的shard
	if !kv.isServeShard[key2shard(args.Key)] {
		DPrintf("[gid-%v-me-%v]--ErrWrongGroup--:from [%v] commandId-%v", kv.gid, kv.me, args.ClientId, args.CommandId)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if reply1, ok := kv.DuplicationCommand(args.ClientId, args.CommandId); ok && args.Op != GET {
		reply.Value, reply.Err = reply1.Value, reply1.Err
		//DPrintf("[%v]client--DuplicationCommand--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
		DPrintf("[gid-%v-me-%v]--DuplicationCommand--:from [%v] commandId-%v", kv.gid, kv.me, args.ClientId, args.CommandId)
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
		DPrintf("[gid-%v-me-%v]--NotLeader--:from [%v] commandId-%v", kv.gid, kv.me, args.ClientId, args.CommandId)

		//DPrintf("[%v]client--NotLeader--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
		kv.mu.Unlock()
		return
	}
	DPrintf("[gid-%v-me-%v]--BeginCommand--:shard-%v-%v-Key-%v-value-%v-resultValue-%v,from [%v] commandId-%v", kv.gid, kv.me, key2shard(args.Key), args.Op, args.Key, args.Value, reply.Value, args.ClientId, args.CommandId)

	ch := kv.newWaitChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("[gid-%v-me-%v]--SuccessCommand--:shard-%v-%v-Key-%v-value-%v-resultValue-%v,from [%v] commandId-%v", kv.gid, kv.me, key2shard(args.Key), args.Op, args.Key, args.Value, reply.Value, args.ClientId, args.CommandId)

		//DPrintf("[%v]client--SuccessCommand--:from [%v] commandId-%v,%v-Key-%v-value-%v-resultValue-%v", kv.me, args.ClientId, args.CommandId, args.Op, args.Key, args.Value, reply.Value)
	case <-time.After(kv.timeout):
		reply.Err = Timeout
		//DPrintf("[%v]client--Timeout--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
	}
	go kv.delWaitChanUL(index)
}

//监听是否有Command apply了
func (kv *ShardKV) listenApply() {
	for !kv.killed() {
		select {
		case applyCommand := <-kv.applyCh:
			{
				if applyCommand.CommandValid {
					op := applyCommand.Command.(Op)
					if op.Opt == PUT || op.Opt == GET || op.Opt == APPEND {
						kv.dealKVCommand(&op, &applyCommand)
					} else {
						kv.dealChangeConfig(&op, &applyCommand)
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

//处理kv数据库相关的消息
func (kv *ShardKV) dealKVCommand(op *Op, applyCommand *raft.ApplyMsg) {
	kv.mu.Lock()
	reply := new(CommandReply)
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

//处理数据迁移等配置变化相关
func (kv *ShardKV) dealChangeConfig(op *Op, applyCommand *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := new(CommandReply)
	reply.ConfigNum = kv.config.Num
	if reply.ConfigNum == op.ConfigNum {
		kv.lastApplyIndex = applyCommand.CommandIndex
		//返回数据并停止服务
		DPrintf("[gid-%v-me-%v]", kv.gid, kv.me)
		if op.Opt == GetShard {
			reply.Data = kv.database.GetShard(op.Shard)
			kv.isServeShard[op.Shard] = false
			kv.rf.Snapshot(kv.lastApplyIndex, kv.getSnapshotL())
		} else if op.Opt == SetShard {
			kv.database.SetShard(op.Data)
		} else if op.Opt == DeleteShard {
			kv.database.DelShard(op.Shard)
		} else if op.Opt == BeginServeShard {
			kv.isServeShard[op.Shard] = true
			kv.rf.Snapshot(kv.lastApplyIndex, kv.getSnapshotL())
		} else if op.Opt == ChangeConfigNum {
			//if kv.gid == 100 {
			//	DPrintf("[100]Begin2")
			//}
			kv.config = kv.mck.Query(op.ConfigNum + 1)
			kv.rf.Snapshot(kv.lastApplyIndex, kv.getSnapshotL())
			//if kv.gid == 100 {
			//	DPrintf("[100]Begin3")
			//}
		}
	}

	currentTerm, isLeader := kv.rf.GetState()
	//_, ok1 := kv.waitChan[applyCommand.CommandIndex]
	//if kv.gid == 101 && op.Opt == ChangeConfigNum {
	//	DPrintf("[gid-101-me-%v]:ok-%v,isLeader-%v,currentTerm-%v,kv.term-%v", kv.me, ok1, isLeader, currentTerm, op.Term)
	//}
	if ch, ok := kv.waitChan[applyCommand.CommandIndex]; ok && isLeader && currentTerm == applyCommand.CommandTerm {

		//if kv.gid == 101 && op.Opt == ChangeConfigNum {
		//	DPrintf("[gid-%v-me-%v]:WantSuccess1", kv.gid, kv.me)
		//}
		ch <- reply
	}
}

//轮询log的大小看是否需要Snapshot
func (kv *ShardKV) listenSnapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.rf.GetPersist().RaftStateSize() >= kv.maxraftstate/4*3 {
			kv.rf.Snapshot(kv.lastApplyIndex, kv.getSnapshotL())
			for s, s2 := range kv.database.Table {
				DPrintf("listenSnapshot:%v--%v\n\n", s, s2)
			}
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

//获得当前状态机的snapshot
func (kv *ShardKV) getSnapshotL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.database.Table)

	//e.Encode(kv.waitChan)
	e.Encode(kv.commandApplyTable)
	//e.Encode(kv.term)
	//e.Encode(kv.lastSnapshotIndex)
	e.Encode(kv.isServeShard)
	e.Encode(kv.config)

	data := w.Bytes()
	return data
}

//重启读取snapshot时初始化状态机
func (kv *ShardKV) setSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Table map[string]string
	//var waitChan map[int]chan *CommandReply
	var commandApplyTable map[int64]*LastApply

	var isServeShard [shardctrler.NShards]bool
	var config shardctrler.Config
	//var lastSnapshotIndex int
	if err := d.Decode(&Table); err != nil {
		DPrintf("[%v][read Table error]-%v", kv.me, err)
	} else if err := d.Decode(&commandApplyTable); err != nil {
		DPrintf("[%v][read commandApplyTable error]-%v", kv.me, err)
	} else if err := d.Decode(&isServeShard); err != nil {
		DPrintf("[%v][read isServeShard error]-%v", kv.me, err)
	} else if err := d.Decode(&config); err != nil {
		DPrintf("[%v][read config error]-%v", kv.me, err)
	} else {
		kv.database.Table = Table
		//kv.waitChan = waitChan
		kv.commandApplyTable = commandApplyTable
		//kv.term = term

		kv.isServeShard = isServeShard
		kv.config = config
		//kv.lastSnapshotIndex = lastSnapshotIndex
		//kv.mu.Lock()
		//for s, s2 := range kv.database.Table {
		//	DPrintf("listenSnapshot:%v--%v\n\n", s, s2)
		//}
		//kv.mu.Unlock()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.database = DataBase{Table: make(map[string]string, 10)}
	kv.waitChan = make(map[int]chan *CommandReply, 10)
	kv.commandApplyTable = make(map[int64]*LastApply, 10)
	kv.timeout = time.Duration(100) * time.Millisecond
	kv.lastApplyIndex = 0

	kv.state = -1
	kv.pullPool = make(map[int]bool)
	kv.pushPool = make(map[int]bool)
	for i, _ := range kv.isServeShard {
		kv.isServeShard[i] = false
	}
	kv.config = shardctrler.Config{Num: 0}

	//读取snapshot
	kv.setSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.

	if maxraftstate != -1 {
		go kv.listenSnapshot()
	}
	go kv.listenApply()
	go kv.listenConfigChange()
	go kv.listenIsNotLeader()
	return kv
}
