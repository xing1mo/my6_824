package mr

import (
	"fmt"
	"log"
)
import "net"
import "net/rpc"
import "net/http"

//实现具体服务
type Coordinator struct {
	// Your definitions here.
	//使用channel储存未完成的任务
	JobChannelMap    chan *Job
	JobChannelReduce chan *Job
	ReducerNum       int
	MapNum           int
	uniqueJobId      int
	//coordinator状态
	//CoordinatorCondition Condition

	//元数据管理相关
	//jobMetaHolder        JobMetaHolder
}

// Your code here -- RPC handlers for the worker to call.
var idNum = 0

func (c Coordinator) generateJobId() int {
	idNum++
	return idNum
}

//Coordinator制作map任务，在一开始程序运行的时候就执行
func (c *Coordinator) makeMapJobs(files []string) {
	for _, file := range files {
		id := c.generateJobId()
		job := Job{
			JobType:    MAP,
			InputFile:  []string{file},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}
		//这下面暂时不需要
		//jobMetaINfo := JobMetaInfo{
		//	condition: JobWaiting,
		//	JobPtr:    &job,
		//}
		//c.jobMetaHolder.putJob(&jobMetaINfo)
		fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job

	}
	fmt.Println("done making map jobs")
	//c.jobMetaHolder.checkJobDone()
}

func (c *Coordinator) Distribute(args *ExampleArgs, reply *Job) error {
	*reply = *<-c.JobChannelMap
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//模拟一个协调者被调用的方法
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// 注册 RPC 服务
	rpc.Register(c)

	// 将 RPC 服务绑定到 HTTP 服务中去
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")

	//sockname := coordinatorSock()
	//os.Remove(sockname)

	//unix进程间通信
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{JobChannelMap: make(chan *Job, 20), ReducerNum: 10}

	// Your code here.
	c.makeMapJobs(files)

	c.server()
	return &c
}
