package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	Timeout        = "Timeout"
	ErrNoChange    = "ErrWrongConfig"
)

type Err string

type Opt string

const (
	GET             Opt = "Get"
	PUT             Opt = "Put"
	APPEND          Opt = "Append"
	GetShard        Opt = "GetShard"
	SetShard        Opt = "SetShard"
	DeleteShard     Opt = "DeleteShard"
	BeginServeShard Opt = "BeginServeShard"
	ChangeConfigNum Opt = "ChangeConfigNum"
)

type CommandArgs struct {
	Key       string
	Value     string
	Op        Opt
	ClientId  int64
	CommandId int
}
type CommandReply struct {
	Err   Err
	Value string

	Data      map[string]string
	ConfigNum int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
