package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int
	commandId int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendCommand(key string, value string, op Opt) string {
	args := CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	DPrintf("[%v]KV-Client--newCommand[%v]--:commandId-%v,%v-%v", ck.clientId, op, ck.commandId, key, value)

	reply := CommandReply{}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok1 := ck.config.Groups[gid]; ok1 {
			DPrintf("[%v]KV-Client--ReGroup[%v]-configNum-%v--:commandId-%v,%v-%v,shard-%v,gid-%v\n\n", ck.clientId, op, ck.config.Num, ck.commandId, key, value, shard, gid)

			// try each server for the shard.
			for {
				//DPrintf("send")
				ok := ck.make_end(servers[ck.leaderId]).Call("ShardKV.ReceiveCommand", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					//DPrintf("[%v]Server--receiveCommand--:commandId-%v,%v-Key-%v-value-%v-resultValue-%v", args.ClientId, args.CommandId, args.Op, args.Key, args.Value, reply.Value)
					ck.commandId++
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == "") {
					break
				}
				ck.leaderId = (ck.leaderId + 1) % len(servers)
				DPrintf("[%v]KV-Client--ReLeader[%v]--:commandId-%v,%v-%v,shard-%v,gid-%v,leaderId-%v\n\n", ck.clientId, op, ck.commandId, key, value, shard, gid, ck.leaderId)
				//当call失败时,可能是某个节点掉线了,也可能是整个group掉了,因此在访问完整个group所有节点命令还没成功执行时,应该尝试更新配置
				if ck.leaderId == 0 {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.leaderId = 0
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.sendCommand(key, "", GET)
}
func (ck *Clerk) Put(key string, value string) {
	ck.sendCommand(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.sendCommand(key, value, APPEND)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
//func (ck *Clerk) Get(key string) string {
//	args := GetArgs{}
//	args.Key = key
//
//	for {
//		shard := key2shard(key)
//		gid := ck.config.Shards[shard]
//		if servers, ok := ck.config.Groups[gid]; ok {
//			// try each server for the shard.
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply GetReply
//				ok := srv.Call("ShardKV.Get", &args, &reply)
//				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
//					return reply.Value
//				}
//				if ok && (reply.Err == ErrWrongGroup) {
//					break
//				}
//				// ... not ok, or ErrWrongLeader
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask controler for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//	}
//
//	return ""
//}
//
////
//// shared by Put and Append.
//// You will have to modify this function.
////
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	args := PutAppendArgs{}
//	args.Key = key
//	args.Value = value
//	args.Op = op
//
//	for {
//		shard := key2shard(key)
//		gid := ck.config.Shards[shard]
//		if servers, ok := ck.config.Groups[gid]; ok {
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply PutAppendReply
//				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
//				if ok && reply.Err == OK {
//					return
//				}
//				if ok && reply.Err == ErrWrongGroup {
//					break
//				}
//				// ... not ok, or ErrWrongLeader
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask controler for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//	}
//}
//
//func (ck *Clerk) Put(key string, value string) {
//	ck.PutAppend(key, value, "Put")
//}
//func (ck *Clerk) Append(key string, value string) {
//	ck.PutAppend(key, value, "Append")
//}
