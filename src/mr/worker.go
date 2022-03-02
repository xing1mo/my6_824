package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//将map中的key进行hash,之后 % NReduce从而选择reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	redf func(string, []string) string) {
	for true {
		job := CallJob()
		switch job.JobType {
		case MAP:
			doMap(mapf, job)
			DPrintf("complete mapjob:", &job)
		case REDUCE:
			doReduce(redf, job)
			DPrintf("complete reducejob:", &job)
		case WaitingJob:
			time.Sleep(1 * time.Second)
		case KillJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", job.JobType))
		}
	}
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}
func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx+1)
}
func doMap(mapf func(string, string) []KeyValue, job *Job) {
	intermediate := []KeyValue{}
	//执行完mapf操作
	for _, filename := range job.InputFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	//将所有key,value分给ReducerNum个文件
	distriKV := make([][]KeyValue, job.ReducerNum)
	for _, kv := range intermediate {
		tmp := ihash(kv.Key) % job.ReducerNum
		distriKV[tmp] = append(distriKV[tmp], kv)
	}

	for idx, l := range distriKV {
		tprname := reduceName(job.JobId, idx)
		tpname := fmt.Sprintf("%v-%v", tprname, job.WorkId)
		job.RNAME = append(job.RNAME, tprname)
		job.Name = append(job.Name, tpname)
		ofile, _ := os.Create(tpname)
		enc := json.NewEncoder(ofile)
		//enc.SetEscapeHTML(false)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("encodeErr-%v", err)
			}
		}
		ofile.Close()
	}
	CallDone(job)
}

func doReduce(redf func(string, []string) string, job *Job) {
	maps := make(map[string][]string)
	//从该reduce对应的所有文件中读取数据(每个map都可能有生成)
	for idx := 1; idx <= job.MapNum; idx++ {
		fileName := reduceName(idx, job.ReduceSeq)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("%v", err)
			continue
		}

		//将文件中的json数据解码
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, redf(k, v)))
	}
	tprname := mergeName(job.ReduceSeq)
	tpname := fmt.Sprintf("%v-%v", job.RNAME, job.WorkId)
	job.RNAME = append(job.RNAME, tprname)
	job.Name = append(job.Name, tpname)

	if err := ioutil.WriteFile(tpname, []byte(strings.Join(res, "")), 0600); err != nil {
		fmt.Printf("%v", err)
	}
	CallDone(job)
}

func CallJob() *Job {
	// declare an argument structure.
	args := Request{WorkId: os.Getpid()}

	// declare a reply structure.
	//设置初始值防止master已退出
	reply := Job{JobType: KillJob}

	call("Coordinator.Distribute", &args, &reply)
	//if ok {
	//	fmt.Printf("call success,reduceSeq:%v\n", reply.ReduceSeq)
	//} else {
	//	fmt.Printf("call failed!\n")
	//}
	return &reply
}
func CallDone(job *Job) {
	reply := ExampleReply{}
	call("Coordinator.JobIsDone", job, &reply)
	//if ok {
	//	fmt.Printf("job complete: %v\n", job.JobId)
	//} else {
	//	fmt.Printf("call failed!\n")
	//}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer client.Close()

	//客户端进行rpc调用
	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
