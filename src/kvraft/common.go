package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Timeout        = "Timeout"
)

type Err string

type Opt string

const (
	GET    Opt = "Get"
	PUT    Opt = "Put"
	APPEND Opt = "Append"
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
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	clientId  int64
	commandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	clientId  int64
	commandId int
}

type GetReply struct {
	Err   Err
	Value string
}
