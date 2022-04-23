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
					//DPrintf("shouldGid:%v,myGid-%v", gid, kv.gid)
					if gid == kv.gid && !kv.isServeShard[shard] {
						kv.pullPool[shard] = true
						if kv.config.Shards[shard] != 0 {
							go kv.pullShard(shard)
						} else {
							nullShard = append(nullShard, shard)
						}

					}
					//收到Pull请求才停止服务,删除数据
					if gid != kv.gid && kv.isServeShard[shard] {
						kv.pushPool[shard] = true
					}
				}
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
		if _, isLeader := kv.rf.GetState(); !isLeader && kv.state != -1 {
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
	Err  string
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
	//op.Term, _ = kv.rf.GetState()
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
		reply.Data, reply.Err = result.Data, OK
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
	DPrintf("[gid-%v-me-%v]:[pullShard]-Begin-shard-%v from gid-%v,config-%v", kv.gid, kv.me, shard, gid, args.ConfigNum)
	reply := PullShardReply{}

	if servers, ok := kv.config.Groups[gid]; ok {
		kv.mu.Unlock()
		leaderId := 0
		for {
			ok := kv.make_end(servers[leaderId]).Call("ShardKV.PushShard", &args, &reply)
			kv.mu.Lock()
			if args.ConfigNum != kv.config.Num || kv.state == -1 || kv.state != kv.config.Num {
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			if ok && reply.Err == OK {
				//成功拉取数据,同步到整个集群
				DPrintf("[gid-%v-me-%v]:[pullShard]-Success-shard-%v from gid-%v,data-%v,config-%v", kv.gid, kv.me, shard, gid, reply.Data, args.ConfigNum)
				op := Op{
					Opt:       SetShard,
					Shard:     args.Shard,
					ConfigNum: args.ConfigNum,
					Data:      reply.Data,
				}
				var index int
				var isLeader bool
				for {
					kv.mu.Lock()
					//op.Term, _ = kv.rf.GetState()
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
				DPrintf("[gid-%v-me-%v]:[pullShard]-Fail-shard-%v from gid-%v,config-%v", kv.gid, kv.me, shard, gid, args.ConfigNum)
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
		Opt:       DeleteShard,
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
	}
	var index int
	var isLeader bool
	//op.Term, _ = kv.rf.GetState()
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
	DPrintf("[gid-%v-me-%v]:[sendGC]-Begin-shard-%v to gid-%v", kv.gid, kv.me, shard, gid)
	reply := GCReply{}

	if servers, ok := kv.config.Groups[gid]; ok {
		kv.mu.Unlock()
		leaderId := 0
		for {
			ok := kv.make_end(servers[leaderId]).Call("ShardKV.GCReceive", &args, &reply)
			kv.mu.Lock()
			if args.ConfigNum != kv.config.Num || kv.state == -1 || kv.state != kv.config.Num {
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			if ok && reply.Err == OK {
				DPrintf("[gid-%v-me-%v]:[sendGC]-Success-shard-%v to gid-%v", kv.gid, kv.me, shard, gid)

				//开始服务
				kv.beginServeShard(args.Shard, args.ConfigNum)

			} else if ok && reply.Err == ErrWrongGroup {
				kv.mu.Lock()
				DPrintf("[gid-%v-me-%v]:[sendGC]-Fail-shard-%v to gid-%v", kv.gid, kv.me, shard, gid)
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
	DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-Begin", kv.gid, kv.me, kv.config.Num)
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
	for {
		kv.mu.Lock()
		//op.Term, _ = kv.rf.GetState()
		index, _, isLeader = kv.rf.Start(op)
		if isLeader == false {
			kv.mu.Unlock()
			return
		}
		//if kv.gid == 100 {
		//	DPrintf("[100]Begin1")
		//}
		ch := kv.newWaitChan(index)
		kv.mu.Unlock()
		select {
		case <-ch:
			DPrintf("[gid-%v-me-%v]:WantSuccess2", kv.gid, kv.me)

			kv.mu.Lock()
			kv.state = -1
			DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-Success", kv.gid, kv.me, kv.config.Num)
			kv.mu.Unlock()
			go kv.delWaitChanUL(index)
			return
		case <-time.After(kv.timeout):
			kv.mu.Lock()
			DPrintf("[gid-%v-me-%v]:[AddConfigNum-%v]-TimeOut", kv.gid, kv.me, kv.config.Num)
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
	DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Begin,config-%v", kv.gid, kv.me, shard, kv.config.Num)
	kv.mu.Unlock()
	for {
		kv.mu.Lock()
		//op.Term, _ = kv.rf.GetState()
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
			DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Success,config-%v", kv.gid, kv.me, shard, kv.config.Num)
			delete(kv.pullPool, shard)
			if kv.checkChangeConfigL(kv.config.Num) {
				go kv.changeConfig(kv.config.Num)
			}
			kv.mu.Unlock()
			go kv.delWaitChanUL(index)
			return
		case <-time.After(kv.timeout):
			DPrintf("[gid-%v-me-%v]:[BeginServeShard-%v]-Timeout", kv.gid, kv.me, shard)
		}
		go kv.delWaitChanUL(index)
	}
}
