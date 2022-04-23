package shardctrler

import (
	"6.824/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type LastApply struct {
	CommandId   int
	ConfigReply *Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	dead int32

	configs []Config // indexed by config num

	//接受某个index对应的消息
	waitChan map[int]chan *Config
	//保存上一次的结果
	commandApplyTable map[int64]*LastApply
	//超时重发时间
	timeout time.Duration
	//提交该命令时的Term
	term int
}

type Op struct {
	Opt       Opt
	ClientId  int64
	CommandId int
	JoinArg   JoinArgs
	LeaveArg  LeaveArgs
	MoveArg   MoveArgs
	QueryArg  QueryArgs
}

func (sc *ShardCtrler) newWaitChan(index int) chan *Config {
	sc.waitChan[index] = make(chan *Config)
	return sc.waitChan[index]
}
func (sc *ShardCtrler) delWaitChanUL() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.waitChan = make(map[int]chan *Config)
}

//判断是否是重复命令
func (sc *ShardCtrler) DuplicationCommand(clientId int64, commandId int) (*Config, bool) {
	if lastApply, ok := sc.commandApplyTable[clientId]; ok && lastApply.CommandId == commandId {
		return lastApply.ConfigReply, true
	}
	return nil, false
}

func (sc *ShardCtrler) ReceiveCommand(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if _, ok := sc.DuplicationCommand(args.ClientId, args.CommandId); ok && args.Op != Query {
		reply.Err = OK
		//DPrintf("[%v]server--DuplicationCommand[%v]--:from [%v] commandId-%v", sc.me, args.Op, args.ClientId, args.CommandId)
		sc.mu.Unlock()
		return
	}
	op := Op{
		Opt:       args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	if args.Op == Join {
		op.JoinArg = args.JoinArg
	} else if args.Op == Leave {
		op.LeaveArg = args.LeaveArg
	} else if args.Op == Query {
		op.QueryArg = args.QueryArg
	} else if args.Op == Move {
		op.MoveArg = args.MoveArg
	}

	var index int
	var isLeader bool
	index, sc.term, isLeader = sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		//DPrintf("[%v]server--NotLeader[%v]--:from [%v] commandId-%v", sc.me, args.Op, args.ClientId, args.CommandId)
		sc.mu.Unlock()
		return
	}
	ch := sc.newWaitChan(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		reply.Config, reply.Err = *result, OK
		//DPrintf("[%v]sc_server--ConfigChange[%v]--:from [%v] commandId-%v,reply-[%v],Command-[%v]", sc.me, args.Op, args.ClientId, args.CommandId, reply, op)
		sc.mu.Lock()
		if args.Op != Query {
			DPrintf("[%v]sc_server--ConfigChange[%v]--: config-%v,Command-%v", sc.me, args.Op, sc.configs[len(sc.configs)-1], op)
		}
		sc.mu.Unlock()
	case <-time.After(sc.timeout):
		reply.Err = Timeout
		//DPrintf("[%v]server--Timeout[%v]--:from [%v] commandId-%v", sc.me, args.Op, args.ClientId, args.CommandId)
	}
	go sc.delWaitChanUL()
}

//监听是否有Command apply了
func (sc *ShardCtrler) listenApply() {
	for !sc.killed() {
		select {
		case applyCommand := <-sc.applyCh:
			{
				reply := new(Config)
				if applyCommand.CommandValid {
					op := applyCommand.Command.(Op)

					sc.mu.Lock()

					//因可能多次提交,重复的commit就不执行了
					if _, ok := sc.DuplicationCommand(op.ClientId, op.CommandId); ok && op.Opt != Query {
						sc.mu.Unlock()
					} else {
						//改变配置
						lastCongigIndex := len(sc.configs) - 1
						if op.Opt == Query {
							command := op.QueryArg
							if command.Num == -1 || command.Num > lastCongigIndex {
								command.Num = lastCongigIndex
							}
							reply = &sc.configs[command.Num]
						} else if op.Opt == Join {
							command := op.JoinArg
							reply = sc.copyConfig(&sc.configs[lastCongigIndex])
							reply.Num++
							for k, v := range command.Servers {
								reply.Groups[k] = v
							}
							sc.balance(reply, &command)

							sc.configs = append(sc.configs, *reply)
						} else if op.Opt == Leave {
							command := op.LeaveArg
							reply = sc.copyConfig(&sc.configs[lastCongigIndex])
							reply.Num++
							for _, k := range command.GIDs {
								delete(reply.Groups, k)
							}
							//删除了的gid其Shards需要重置
							for shard, gid := range reply.Shards {
								if _, ok := reply.Groups[gid]; !ok {
									reply.Shards[shard] = 0
								}
							}
							sc.balance(reply, nil)

							sc.configs = append(sc.configs, *reply)
						} else {
							command := op.MoveArg
							reply = sc.copyConfig(&sc.configs[lastCongigIndex])
							reply.Num++
							reply.Shards[command.Shard] = command.GID
							sc.configs = append(sc.configs, *reply)
						}

						//不是leader不提交结果
						currentTerm, isLeader := sc.rf.GetState()
						//保存上一次执行结果以及通知返回结果
						if op.Opt != Query {
							sc.commandApplyTable[op.ClientId] = &LastApply{
								CommandId:   op.CommandId,
								ConfigReply: nil,
							}
						}

						//仅在我等结果,并且我是Leader,term没有改变的情况下才返回,否则index被覆盖了导致错误
						if ch, ok := sc.waitChan[applyCommand.CommandIndex]; ok && isLeader && currentTerm == sc.term {
							ch <- reply
						}
						sc.mu.Unlock()
					}
				}
			}
		}
	}
}

//平衡shards,可以考虑每次从大的移一个到小的
func (sc *ShardCtrler) balance(reply *Config, command *JoinArgs) {
	//DPrintf("[%v]--BeforeBalance--:shards-%v", sc.me, reply.Shards)
	if len(reply.Groups) == 0 {
		for i := 0; i < len(reply.Shards); i++ {
			reply.Shards[i] = 0
		}
		//DPrintf("[%v]--AfterBalance--:shards-%v", sc.me, reply.Shards)
		return
	}
	//记录每个groupId被分配的次数
	cntGroup := make(map[int]int)
	for gid, _ := range reply.Groups {
		cntGroup[gid] = 0
	}
	for _, gid := range reply.Shards {
		if gid == 0 {
			continue
		}
		cntGroup[gid]++
	}

	//最终需要被分配的次数
	balanceCnt := NShards / len(reply.Groups)

	//哪些group少了需要添加
	lessGroup := make([]int, 0)
	//哪些group少了,还能继续加1
	tooLessGroup := make([]int, 0)
	for k, v := range cntGroup {
		if v < balanceCnt {
			for i := 1; i <= balanceCnt-v; i++ {
				lessGroup = append(lessGroup, k)
			}
		}
		if v <= balanceCnt {
			tooLessGroup = append(tooLessGroup, k)
		}
	}

	//map访问结果是乱序的
	sort.Ints(lessGroup)
	sort.Ints(tooLessGroup)

	lessId := 0
	for shard, gid := range reply.Shards {
		if gid == 0 && lessId < len(lessGroup) {
			reply.Shards[shard] = lessGroup[lessId]
			lessId++
		} else if cntGroup[gid] > balanceCnt+1 && lessId < len(lessGroup) {
			cntGroup[gid]--
			reply.Shards[shard] = lessGroup[lessId]
			lessId++
		}
		if lessId >= len(lessGroup) {
			break
		}
	}

	//less满了还有的没降到balence+1,让接受者从balance改为balance+1
	if lessId >= len(lessGroup) {
		tooLessId := 0
		for shard, gid := range reply.Shards {
			if gid == 0 && tooLessId < len(tooLessGroup) {
				reply.Shards[shard] = tooLessGroup[tooLessId]
				tooLessId++
			} else if cntGroup[gid] > balanceCnt+1 && tooLessId < len(tooLessGroup) {
				cntGroup[gid]--
				reply.Shards[shard] = tooLessGroup[tooLessId]
				tooLessId++
			}
		}
	} else if lessId < len(lessGroup) {
		//某些group的shard还是少了,需要继续补齐,发送者从balance+1分出来变为balance
		for shard, gid := range reply.Shards {
			if cntGroup[gid] == balanceCnt+1 && lessId < len(lessGroup) {
				cntGroup[gid]--
				reply.Shards[shard] = lessGroup[lessId]
				lessId++
			}
			if lessId >= len(lessGroup) {
				break
			}
		}
	}
	//DPrintf("[%v]--AfterBalance--:shards-%v", sc.me, reply.Shards)
}
func (sc *ShardCtrler) copyConfig(config *Config) *Config {
	result := &Config{
		Num: config.Num,
	}
	result.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		result.Groups[k] = v
	}
	for i, shard := range config.Shards {
		result.Shards[i] = shard
	}
	return result
}

//func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
//	sc.mu.Lock()
//	if _, ok := sc.DuplicationCommand(args.ClientId, args.CommandId); ok {
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--DuplicationCommand[Join]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	op := Op{
//		Opt:     Join,
//		Command: *args,
//	}
//	var index int
//	var isLeader bool
//	index, sc.term, isLeader = sc.rf.Start(op)
//	if isLeader == false {
//		reply.WrongLeader = true
//		DPrintf("[%v]server--NotLeader[Join]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	ch := sc.newWaitChan(index)
//	sc.mu.Unlock()
//	select {
//	case <-ch:
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--SuccessCommand[Join]--:from [%v] commandId-%v,servers-%v", sc.me, args.ClientId, args.CommandId, args.Servers)
//	case <-time.After(sc.timeout):
//		reply.WrongLeader, reply.Err = false, Timeout
//		DPrintf("[%v]server--Timeout[Join]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//	}
//	go sc.delWaitChanUL()
//}
//
//func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
//	sc.mu.Lock()
//	if _, ok := sc.DuplicationCommand(args.ClientId, args.CommandId); ok {
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--DuplicationCommand[Leave]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	op := Op{
//		Opt:     Leave,
//		Command: *args,
//	}
//	var index int
//	var isLeader bool
//	index, sc.term, isLeader = sc.rf.Start(op)
//	if isLeader == false {
//		reply.WrongLeader = true
//		DPrintf("[%v]server--NotLeader[Leave]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	ch := sc.newWaitChan(index)
//	sc.mu.Unlock()
//	select {
//	case <-ch:
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--SuccessCommand[Leave]--:from [%v] commandId-%v,GIDs-%v", sc.me, args.ClientId, args.CommandId, args.GIDs)
//	case <-time.After(sc.timeout):
//		reply.WrongLeader, reply.Err = false, Timeout
//		DPrintf("[%v]server--Timeout[Leave]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//	}
//	go sc.delWaitChanUL()
//}
//
//func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
//	sc.mu.Lock()
//	if _, ok := sc.DuplicationCommand(args.ClientId, args.CommandId); ok {
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--DuplicationCommand[Move]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	op := Op{
//		Opt:     Move,
//		Command: *args,
//	}
//	var index int
//	var isLeader bool
//	index, sc.term, isLeader = sc.rf.Start(op)
//	if isLeader == false {
//		reply.WrongLeader = true
//		DPrintf("[%v]server--NotLeader[Move]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	ch := sc.newWaitChan(index)
//	sc.mu.Unlock()
//	select {
//	case <-ch:
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--SuccessCommand[Move]--:from [%v] commandId-%v,shard-%v,gid-%v", sc.me, args.ClientId, args.CommandId, args.Shard, args.GID)
//	case <-time.After(sc.timeout):
//		reply.WrongLeader, reply.Err = false, Timeout
//		DPrintf("[%v]server--Timeout[Move]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//	}
//	go sc.delWaitChanUL()
//}
//
//func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
//	sc.mu.Lock()
//	if reply1, ok := sc.DuplicationCommand(args.ClientId, args.CommandId); ok {
//		reply.Config, reply.WrongLeader, reply.Err = *reply1, false, OK
//		DPrintf("[%v]server--DuplicationCommand[Query]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	op := Op{
//		Opt:     Move,
//		Command: *args,
//	}
//	var index int
//	var isLeader bool
//	index, sc.term, isLeader = sc.rf.Start(op)
//	if isLeader == false {
//		reply.WrongLeader = true
//		DPrintf("[%v]server--NotLeader[Query]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//		sc.mu.Unlock()
//		return
//	}
//	ch := sc.newWaitChan(index)
//	sc.mu.Unlock()
//	select {
//	case <-ch:
//		reply.WrongLeader, reply.Err = false, OK
//		DPrintf("[%v]server--SuccessCommand[Query]--:from [%v] commandId-%v,num-%v", sc.me, args.ClientId, args.CommandId, args.Num)
//	case <-time.After(sc.timeout):
//		reply.WrongLeader, reply.Err = false, Timeout
//		DPrintf("[%v]server--Timeout[Query]--:from [%v] commandId-%v", sc.me, args.ClientId, args.CommandId)
//	}
//	go sc.delWaitChanUL()
//}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChan = make(map[int]chan *Config, 10)
	sc.commandApplyTable = make(map[int64]*LastApply, 10)
	sc.timeout = time.Duration(200) * time.Millisecond

	go sc.listenApply()
	return sc
}
