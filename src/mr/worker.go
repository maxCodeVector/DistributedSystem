package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type TaskDefinition struct {
	TaskID     int
	AttemptID  int
	TaskType   TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskResult struct {
	TaskID    int
	AttemptID int
	TaskType  TaskType
	FilePaths []string
}

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	TERMINAL
)

type MapTask struct {
	filePath string
	content  string
	nReduce  int // the number of reduce tasks
}

type ReduceTask struct {
	reducerIndex       int      // the key bucket calculated by ihash(key) % NReduce
	intermedicateFiles []string // each file store key value pairs
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task := getTask()
		if task == nil {
			// connection failed, may sleep for a while and try again
			time.Sleep(time.Second)
			continue
		}
		switch task.TaskType {
		case MAP:
			performMapTask(task, mapf)
		case REDUCE:
			performReduceTask(task, reducef)
		case TERMINAL:
			// no more tasks, exit
			log.Printf("no more tasks, exit")
			return
		default:
			log.Fatalf("unknown task type %v", task.TaskType)
		}

	}

}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func performMapTask(task *TaskDefinition, mapf func(string, string) []KeyValue) {
	// do map task
	mapTask := task.MapTask
	content := readFile(mapTask.filePath)
	values := mapf(mapTask.filePath, content)

	filePathPrefix := fmt.Sprintf("mr-%d-%d", task.TaskID, task.AttemptID)
	resultFilePaths := saveKeyValueToFiles(filePathPrefix, mapTask.nReduce, values)
	SendMapTaskResult(task.TaskID, task.AttemptID, resultFilePaths)
}

func performReduceTask(task *TaskDefinition, reducef func(string, []string) string) {
	// do reduce task
	reduceTask := task.ReduceTask
	intermediateFiles := reduceTask.intermedicateFiles
	// read the files and merge them into a map
	kvMap := make(map[string][]string)
	for _, filePath := range intermediateFiles {
		content := readFile(filePath)
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			if len(kv) != 2 {
				log.Fatalf("invalid key value pair %v", line)
			}
			key := kv[0]
			value := kv[1]
			kvMap[key] = append(kvMap[key], value)
		}
	}

	resultFilePath := fmt.Sprintf("mr-out-%d-%d", task.TaskID, task.AttemptID)
	resultFile, err := os.Create(resultFilePath)
	if err != nil {
		log.Fatalf("cannot create %v", resultFilePath)
	}
	defer resultFile.Close()

	for key, values := range kvMap {
		resultValue := reducef(key, values)
		resultFile.WriteString(fmt.Sprintf("%v %v\n", key, resultValue))
	}

	SendReduceTaskResult(task.TaskID, task.AttemptID, resultFilePath)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func getTask() *TaskDefinition {

	// declare an argument structure.
	taskDef := TaskDefinition{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AcquireTask", &ExampleArgs{}, &taskDef)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("get Task %v\n", taskDef)
		return &taskDef
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// splite the values into different files
// and return the file paths
// the key is put in ith file where i = ihash(key) % nReduce
func saveKeyValueToFiles(fileNamePrefix string, nReduce int, values []KeyValue) []string {
	// split the values into different files
	intermidiateFiles := make([]string, nReduce)
	filePointer := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		intermidiateFiles[i] = fmt.Sprintf("%s-%d", fileNamePrefix, i)
		file, err := os.OpenFile(
			intermidiateFiles[i], os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", intermidiateFiles[i])
		}
		filePointer[i] = file
		defer filePointer[i].Close()
	}
	for _, v := range values {
		// write the values to a file
		fileIndex := ihash(v.Key) % nReduce
		file := filePointer[fileIndex]
		_, err := file.WriteString(fmt.Sprintf("%s %s\n", v.Key, v.Value))
		if err != nil {
			log.Fatalf("cannot write to %v", intermidiateFiles[fileIndex])
		}
	}
	return intermidiateFiles
}

func SendMapTaskResult(taskId int, attemptID int, resultFilePaths []string) {

	// declare an argument structure.
	taskResult := TaskResult{
		TaskID:    taskId,
		AttemptID: attemptID,
		TaskType:  MAP,
		FilePaths: resultFilePaths,
	}

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.SendTaskResult", &taskResult, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("send Map TaskResult %v\n", taskResult)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func SendReduceTaskResult(taskId int, attemptID int, resultPath string) {

	// declare an argument structure.
	taskResult := TaskResult{
		TaskID:    taskId,
		AttemptID: attemptID,
		TaskType:  REDUCE,
		FilePaths: []string{resultPath},
	}

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.SendTaskResult", &taskResult, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("send Reduce TaskResult %v\n", taskResult)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
