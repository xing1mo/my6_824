package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	leaderId  int
	commandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendCommand(op Opt, command interface{}) Config {
	args := &CommandArgs{
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	if op == Join {
		args.JoinArg = command.(JoinArgs)
	} else if op == Leave {
		args.LeaveArg = command.(LeaveArgs)
	} else if op == Query {
		args.QueryArg = command.(QueryArgs)
	} else if op == Move {
		args.MoveArg = command.(MoveArgs)
	}
	//DPrintf("[%v]SC-Client--newCommand[%v]--:commandId-%v,%v\n\n", ck.clientId, op, ck.commandId, command)

	reply := CommandReply{}
	for true {
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.ReceiveCommand", args, &reply)
		//DPrintf("ok-%v,Err-%v", ok, reply.Err)
		if ok && reply.Err == OK {
			ck.commandId++
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
	return Config{}
}

func (ck *Clerk) Query(num int) Config {
	return ck.sendCommand(Query, QueryArgs{
		Num: num,
	})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.sendCommand(Join, JoinArgs{
		Servers: servers,
	})
}

func (ck *Clerk) Leave(gids []int) {
	ck.sendCommand(Leave, LeaveArgs{
		GIDs: gids,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.sendCommand(Move, MoveArgs{
		Shard: shard,
		GID:   gid,
	})
}
