package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
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
	// You'll have to add code here.
	ck.mu.Lock()
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	ck.mu.Unlock()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.sendCommand(key, "", GET)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.sendCommand(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.sendCommand(key, value, APPEND)
}

func (ck *Clerk) sendCommand(key string, value string, op Opt) string {
	ck.mu.Lock()
	args := CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	DPrintf("[%v]server--newCommand--:commandId-%v,%v-%v-%v\n\n", ck.clientId, ck.commandId, op, key, value)
	ck.mu.Unlock()

	reply := CommandReply{}
	for true {
		ck.mu.Lock()
		//DPrintf("[%v]Server--sendCommand--:commandId-%v,%v-Key-%v-value-%v", args.ClientId, args.CommandId, args.Op, args.Key, args.Value)
		ck.mu.Unlock()
		ok := ck.servers[ck.leaderId].Call("KVServer.ReceiveCommand", &args, &reply)
		//DPrintf("ok-%v,Err-%v", ok, reply.Err)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.mu.Lock()
			//DPrintf("[%v]Server--receiveCommand--:commandId-%v,%v-Key-%v-value-%v-resultValue-%v", args.ClientId, args.CommandId, args.Op, args.Key, args.Value, reply.Value)
			ck.commandId++
			ck.mu.Unlock()
			return reply.Value
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
	return ""
}
