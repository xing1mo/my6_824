package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type JobCondition int

const (
	JobWaiting JobCondition = 1
	JobWorking JobCondition = 2
	JobDone    JobCondition = 3
)

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}
type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	mu1.Lock()
	defer mu1.Unlock()
	jobId := JobInfo.JobPtr.JobId
	meta, _ := j.MetaMap[jobId]
	if meta != nil {
		fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
}
func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo) {
	meta, _ := j.MetaMap[jobId]
	if meta == nil {
		return false, nil
	} else {
		return true, meta
	}
}

//使job转入工作阶段
func (j *JobMetaHolder) fireTheJob(jobId int) bool {
	mu1.Lock()
	defer mu1.Unlock()
	ok, jobInfo := j.getJobMetaInfo(jobId)
	if !ok || jobInfo.condition != JobWaiting {
		return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true
}
func (j *JobMetaHolder) checkJobDone() bool {
	mu1.RLock()
	defer mu1.RUnlock()
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MAP {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	DPrintf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	return (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0)
}

type Condition int

const (
	MapPhase    Condition = 1
	ReducePhase Condition = 2
	AllDone     Condition = 3
)

//实现具体服务
type Coordinator struct {
	// Your definitions here.
	//使用channel储存未完成的任务
	JobChannelMap    chan *Job
	JobChannelReduce chan *Job
	ReducerNum       int
	MapNum           int

	//coordinator状态
	CoordinatorCondition Condition

	//多久检查任务崩溃
	CrashTimeCheck  time.Duration
	MaxTaskWorkTime time.Duration

	//元数据管理相关
	jobMetaHolder JobMetaHolder
}

// Your code here -- RPC handlers for the worker to call.

//检查任务崩溃
func (c *Coordinator) checkJobCrash(jobId int) {
	for true {
		mu1.RLock()
		_, jobMetaInfo := c.jobMetaHolder.getJobMetaInfo(jobId)
		//DPrintf("checkCrash:%v", *jobMetaInfo)
		switch jobMetaInfo.condition {
		case JobWaiting:
		case JobWorking:
			if time.Since(jobMetaInfo.StartTime) > c.MaxTaskWorkTime {
				DPrintf("crash job %d", jobId)
				jobMetaInfo.condition = JobWaiting
				if jobMetaInfo.JobPtr.JobType == MAP {
					c.JobChannelMap <- jobMetaInfo.JobPtr
				} else {
					c.JobChannelReduce <- jobMetaInfo.JobPtr
				}
			}
		case JobDone:
			mu1.RUnlock()
			return
		default:
			panic(fmt.Sprintf("panic checkJobCrash %v,unexpect condition", jobId))
		}
		mu1.RUnlock()
		time.Sleep(c.CrashTimeCheck)
	}
}
func (c *Coordinator) generateJobId() int {
	idNum++
	return idNum
}

func (c *Coordinator) nextPhase() {
	mu.Lock()
	if c.CoordinatorCondition == MapPhase {
		c.CoordinatorCondition = ReducePhase
		mu.Unlock()
		c.makeReduceJobs()
	} else if c.CoordinatorCondition == ReducePhase {
		c.CoordinatorCondition = AllDone
		mu.Unlock()
	}

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

		jobMetaINfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaINfo)

		DPrintf("making map job :", &job)
		c.JobChannelMap <- &job
		go c.checkJobCrash(job.JobId)
	}
	DPrintf("done making map jobs")
	c.jobMetaHolder.checkJobDone()
}

//Coordinator制作reduce任务，在转为reduce阶段后执行
func (c *Coordinator) makeReduceJobs() {
	myMapNum := idNum
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		job := Job{
			JobType:    REDUCE,
			JobId:      id,
			ReduceSeq:  i,
			MapNum:     myMapNum,
			ReducerNum: c.ReducerNum,
		}
		jobMetaINfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaINfo)
		DPrintf("making reduce job :", &job)
		c.JobChannelReduce <- &job
		go c.checkJobCrash(job.JobId)
	}
	fmt.Println("done making reduce jobs")
	c.jobMetaHolder.checkJobDone()
}

//work请求任务分发
func (c *Coordinator) Distribute(args *ExampleArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	DPrintf("--coordinator get a request from worker--")
	if c.CoordinatorCondition == MapPhase {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaitingJob
			if c.jobMetaHolder.checkJobDone() {
				go c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			//fmt.Println(reply.ReduceSeq)
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaitingJob
			if c.jobMetaHolder.checkJobDone() {
				go c.nextPhase()
			}
			return nil
		}
	} else {
		reply.JobType = KillJob
	}
	return nil
}

//woker完成任务
func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	mu1.Lock()
	switch args.JobType {
	case MAP:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			mu1.Unlock()
			DPrintf("Map task on %d complete\n", args.JobId)
		} else {
			fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	case REDUCE:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			mu1.Unlock()
			DPrintf("Reduce task on %d complete\n", args.JobId)
		} else {
			fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	default:
		panic("wrong job done")
	}
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
	mu.RLock()
	// Your code here.
	if c.CoordinatorCondition == AllDone {
		ret = true
	}
	mu.RUnlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
var idNum int
var mu sync.RWMutex
var mu1 sync.RWMutex

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{JobChannelMap: make(chan *Job, 20),
		jobMetaHolder:        JobMetaHolder{make(map[int]*JobMetaInfo, 20)},
		JobChannelReduce:     make(chan *Job, 20),
		ReducerNum:           nReduce,
		CrashTimeCheck:       time.Second * 5,
		MaxTaskWorkTime:      time.Second * 10,
		CoordinatorCondition: MapPhase}
	idNum = 0
	// Your code here.
	c.makeMapJobs(files)

	c.server()
	return &c
}
