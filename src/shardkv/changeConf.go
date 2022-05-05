package shardkv

import (
	"time"
)

//type GroupState int
//
//const (
//	Work         GroupState = 1 //正常
//	ChangeConfig GroupState = 2 //正在更改config
//)

//type ShardState int
//
//const (
//	Serve    ShardState = 1 //服务
//	NotServe ShardState = 2 //不服务
//	GC       ShardState = 3 //需要垃圾回收
//)
//
//type ShardData struct {
//	State     ShardState
//	ConfigNum int
//}

//监听是否有配置变更
func (kv *ShardKV) listenConfigChange() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && kv.state == -1 {
			conf := kv.mck.Query(kv.config.Num + 1)
			if conf.Num == kv.config.Num+1 {
				//新增的shard
				kv.state = kv.config.Num
				kv.pullPool = make(map[int]bool)
				kv.pushPool = make(map[int]bool)

				nullShard := make([]int, 0)
				for shard, gid := range conf.Shards {
					//需要得到的数据,gc后才开始新的服务

					//后发现有的group还没成功更新配置版本，其他group先一步更新了版本。考虑原因：
					//group1已经收到GCReceive消息，清空了push池，触发更新配置版本操作，
					//但GC消息的发送group对应Leader这时候挂了，使得对应shard并没有收到GC成功的消息，
					//也就没能开始服务。新晋升的Leader的pull池会放入该shard，但配置版本已经过低拿不到数据了。
					//通过在config落后情况下仅执行发送者操作解决
					if gid == kv.gid && !kv.isServeShard[shard] {
						kv.pullPool[shard] = true

						//有数据但无服务,重新尝试GC,避免因GC成功但没收到应答导致config版本不一致
						if kv.database.HasShard(shard) {
							DPrintf("[gid-%v-me-%v]:[sendGC]-BecauseHasShard-shard-%v ,[configNum-%v]\n\n", kv.gid, kv.me, shard, kv.config.Num)

							go kv.sendGC(shard)
						} else if kv.config.Shards[shard] != 0 {
							go kv.pullShard(shard)
						} else {
							nullShard = append(nullShard, shard)
						}

					}
					//收到Pull请求才停止服务,删除数据
					if gid != kv.gid && (kv.isServeShard[shard] || (!kv.isServeShard[shard] && kv.database.HasShard(shard))) {
						//当两个group在成功pull数据后都挂掉了，没能开始GC。后两Group新选举的Leader都重新开始configChange，g1是pull端，g2是push端。g2因之前在pull数据过程中停止了服务，重新选举后不会进入push池，导致版本增加，但没能GC，出现数据未删除的情况。应在push池生成过程中同时判断未服务但有数据的情况
						kv.pushPool[shard] = true
					}
				}

				//测试打印代码
				pullShard := make([]int, 0)
				pushShard := make([]int, 0)
				for shard, _ := range kv.pullPool {
					pullShard = append(pullShard, shard)
				}
				for shard, _ := range kv.pushPool {
					pushShard = append(pushShard, shard)
				}
				DPrintf("[gid-%v-me-%v]:[NewStateChange-%v],pullPool-%v,pushPool-%v,null-%v\n\n", kv.gid, kv.me, kv.state, pullShard, pushShard, nullShard)

				for _, shard := range nullShard {
					go kv.beginServeShard(shard, kv.config.Num)
				}
				if len(nullShard) == 0 && len(kv.pushPool) == 0 && len(kv.pullPool) == 0 {
					if kv.checkChangeConfigL(kv.config.Num) {
						go kv.changeConfig(kv.config.Num)
					}
				}

			}

		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
func (kv *ShardKV) listenIsNotLeader() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); (!isLeader || kv.killed()) && kv.state != -1 {
			kv.state = -1
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

type PullShardArgs struct {
	Shard     int
	ConfigNum int
}
type PullShardReply struct {
	Data map[string]string
	//数据迁移时应该还要迁移一个shard对应的去重表
	CommandApplyTable map[int64]LastApply
	Err               string
}

//向pull端回复数据
func (kv *ShardKV) PushShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	if kv.state == -1 || kv.state < args.ConfigNum {
		reply.Err = ErrNoChange
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum < kv.state {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	op := Op{
		Opt:       GetShard,
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
	}
	var index int
	var isLeader bool
	index, _, isLeader = kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.newWaitChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Data, reply.CommandApplyTable, reply.Err = result.Data, result.CommandApplyTable, OK
		kv.mu.Lock()
		DPrintf("[gid-%v-me-%v]:[EndServeShard-%v],[configNum-%v]\n\n", kv.gid, kv.me, args.Shard, kv.config.Num)
		kv.mu.Unlock()
	case <-time.After(kv.timeout):
		reply.Err = Timeout
		//DPrintf("[%v]client--Timeout--:from [%v] commandId-%v", kv.me, args.ClientId, args.CommandId)
	}
	go kv.delWaitChanUL(index)
}

//向gid请求shard的数据
func (kv *ShardKV) pullShard(shard int) {
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	args := PullShardArgs{
		Shard:     shard,
		ConfigNum: kv.config.Num,
	}
	//DPrintf("[gid-%v-me-%v]:[pullShard]-Begin-shard-%v from gid-%v,config-%v", kv.gid, kv.me, shard, gid, args.ConfigNum)
	reply := PullShardReply{}

	if servers, ok := kv.config.Groups[gid]; ok {
		kv.mu.Unlock()
		leaderId := 0
		for !kv.killed() {
			ok := kv.make_end(servers[leaderId]).Call("ShardKV.PushShard", &args, &reply)
			kv.mu.Lock()
			if args.ConfigNum != kv.config.Num || kv.state == -1 || kv.state != kv.config.Num {
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			if ok && reply.Err == OK {
				//成功拉取数据,同步到整个集群
				DPrintf("[gid-%v-me-%v]:[pullShard]-Success-shard-%v from gid-%v,data-%v,CommandApplyTable-%v,[configNum-%v]\n\n", kv.gid, kv.me, shard, gid, reply.Data, reply.CommandApplyTable, args.ConfigNum)
				op := Op{
					Opt:               SetShard,
					Shard:             args.Shard,
					ConfigNum:         args.ConfigNum,
					Data:              reply.Data,
					CommandApplyTable: reply.CommandApplyTable,
				}
				var index int
				var isLeader bool
				for !kv.killed() {
					kv.mu.Lock()
					index, _, isLeader = kv.rf.Start(op)
					if isLeader == false {
						kv.mu.Unlock()
						return
					}
					ch := kv.newWaitChan(index)
					kv.mu.Unlock()
					select {
					case <-ch: //数据同步成功,开始gc
						go kv.sendGC(shard)
						go kv.delWaitChanUL(index)
						return
					case <-time.After(kv.timeout):
						reply.Err = Timeout
					}
					go kv.delWaitChanUL(index)
				}

			} else if ok && reply.Err == ErrWrongGroup {
				kv.mu.Lock()
				DPrintf("[gid-%v-me-%v]:[pullShard]-Fail-shard-%v from gid-%v,[configNum-%v]\n\n", kv.gid, kv.me, shard, gid, args.ConfigNum)
				kv.mu.Unlock()
				return
			} else if ok && reply.Err == ErrNoChange {
				//对面还没进入configChange状态
				time.Sleep(100 * time.Millisecond)
			}
			leaderId = (leaderId + 1) % len(servers)
		}
	} else {
		kv.state = -1
		kv.mu.Unlock()
	}
}

type GCArgs struct {
	Shard     int
	ConfigNum int
}
type GCReply struct {
	Err string
}

//回复GC
func (kv *ShardKV) GCReceive(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	//因网络问题收到GC但没回复导致重发时版本落后,GC接受方不会进入configChange状态,需放前面
	if args.ConfigNum < kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.state == -1 || kv.state < args.ConfigNum {
		reply.Err = ErrNoChange
		kv.mu.Unlock()
		return
	}

	op := Op{
		Opt:       DeleteShard,
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
	}
	var index int
	var isLeader bool
	index, _, isLeader = kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.newWaitChan(index)
	kv.mu.Unlock()
	select {
	case <-ch:
		{
			reply.Err = OK
			kv.mu.Lock()
			DPrintf("[gid-%v-me-%v]:[DeleteData]-Success-shard-%v,[configNum-%v]\n\n", kv.gid, kv.me, args.Shard, kv.config.Num)

			delete(kv.pushPool, args.Shard)
			if kv.checkChangeConfigL(kv.config.Num) {
				go kv.changeConfig(kv.config.Num)
			}
			kv.mu.Unlock()
		}

	case <-time.After(kv.timeout):
		reply.Err = Timeout
	}
	go kv.delWaitChanUL(index)
}

//像gid发送GC请求
func (kv *ShardKV) sendGC(shard int) {
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	args := GCArgs{
		Shard:     shard,
		ConfigNum: kv.config.Num,
	}
	//DPrintf("[gid-%v-me-%v]:[sendGC]-Begin-shard-%v from gid-%v", kv.gid, kv.me, shard, gid)
	reply := GCReply{}

	if servers, ok := kv.config.Groups[gid]; ok {
		kv.mu.Unlock()
		leaderId := 0
		for !kv.killed() {
			ok := kv.make_end(servers[leaderId]).Call("ShardKV.GCReceive", &args, &reply)
			kv.mu.Lock()
			if args.ConfigNum != kv.config.Num || kv.state == -1 || kv.state != kv.config.Num {
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			if ok && reply.Err == OK {
				kv.mu.Lock()
				DPrintf("[gid-%v-me-%v]:[sendGC]-Success-shard-%v to gid-%v,[configNum-%v]\n\n", kv.gid, kv.me, shard, gid, kv.config.Num)
				kv.mu.Unlock()
				//开始服务
				go kv.beginServeShard(args.Shard, args.ConfigNum)
				return

			} else if ok && reply.Err == ErrWrongGroup {
				kv.mu.Lock()
				DPrintf("[gid-%v-me-%v]:[sendGC]-Fail-shard-%v to gid-%v,[configNum-%v]\n\n", kv.gid, kv.me, shard, gid, kv.config.Num)

				//版本落后了,但也重新尝试开启服务,避免因GC成功但没收到应答导致config版本不一致
				if kv.pullPool[args.Shard] {
					go kv.beginServeShard(args.Shard, args.ConfigNum)
				}
				kv.mu.Unlock()
				return
			} else if ok && reply.Err == ErrNoChange {
				//对面还没进入configChange状态
				time.Sleep(10 * time.Millisecond)
			}
			leaderId = (leaderId + 1) % len(servers)
		}
	} else {
		kv.state = -1
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) checkChangeConfigL(configNum int) bool {
	if len(kv.pushPool) != 0 || len(kv.pullPool) != 0 || kv.config.Num != configNum || kv.state != kv.config.Num {
		return false
	}
	//DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-Begin", kv.gid, kv.me, kv.config.Num)
	return true
}

//增加到下一版本config
func (kv *ShardKV) changeConfig(configNum int) {
	op := Op{
		Opt:       ChangeConfigNum,
		ConfigNum: configNum,
	}
	var index int
	var isLeader bool
	for !kv.killed() {
		kv.mu.Lock()
		index, _, isLeader = kv.rf.Start(op)
		if isLeader == false {
			kv.mu.Unlock()
			return
		}
		ch := kv.newWaitChan(index)
		kv.mu.Unlock()
		select {
		case <-ch:
			//DPrintf("[gid-%v-me-%v]:WantSuccess2", kv.gid, kv.me)
			kv.mu.Lock()
			kv.state = -1
			DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-Success\n\n", kv.gid, kv.me, kv.config.Num)
			kv.mu.Unlock()
			go kv.delWaitChanUL(index)
			return
		case <-time.After(kv.timeout):
			kv.mu.Lock()
			DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-TimeOut\n\n", kv.gid, kv.me, kv.config.Num)
			kv.mu.Unlock()
		}
		go kv.delWaitChanUL(index)
	}
}

//开始服务
func (kv *ShardKV) beginServeShard(shard int, configNum int) {
	op := Op{
		Opt:       BeginServeShard,
		Shard:     shard,
		ConfigNum: configNum,
	}
	var index int
	var isLeader bool
	kv.mu.Lock()
	//DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Begin,config-%v", kv.gid, kv.me, shard, kv.config.Num)
	kv.mu.Unlock()
	for !kv.killed() {
		kv.mu.Lock()
		index, _, isLeader = kv.rf.Start(op)
		if isLeader == false {
			kv.mu.Unlock()
			return
		}
		ch := kv.newWaitChan(index)
		kv.mu.Unlock()
		select {
		case <-ch:
			kv.mu.Lock()
			DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Success,[configNum-%v]\n\n", kv.gid, kv.me, shard, kv.config.Num)
			delete(kv.pullPool, shard)
			if kv.checkChangeConfigL(kv.config.Num) {
				go kv.changeConfig(kv.config.Num)
			}
			kv.mu.Unlock()
			go kv.delWaitChanUL(index)
			return
		case <-time.After(kv.timeout):
			_, isd := kv.rf.GetState()
			kv.mu.Lock()
			//有一定概率出现config不再更新的情况。首先考虑到原因是某个Leader节点在拉取完数据但还没开始服务时挂掉了，或者成为了单独的网络分区，但保持了Leader的身份，需要通过Leader触发的开始服务操作没能成功执行，但这里实际上只会使得网络分区的节点重复执行操作，一直Timeout无法成功，新晋升的Leader会重新开始configChange。
			DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Timeout,isLeader-%v,[configNum-%v]\n\n", kv.gid, kv.me, shard, isd, kv.config.Num)
			kv.mu.Unlock()
		}
		go kv.delWaitChanUL(index)
	}
}
