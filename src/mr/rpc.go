package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//类似枚举类型
type JobType int

const (
	MAP        JobType = 1
	REDUCE     JobType = 2
	WaitingJob JobType = 3
	KillJob    JobType = 4
)

type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReduceSeq  int
	MapNum     int
	ReducerNum int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	//返回调用者的数字用户id
	s += strconv.Itoa(os.Getuid())
	return s
}
