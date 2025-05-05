package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mapTasks    *list.List    // ready Tasks list for Map tasks
	reduceTasks map[int]*Task // ready Tasks map bteween reducer index and its tasks for Reduce tasks

	runningTasks  map[int]*Task // map between task id and Task
	completeTasks []*Task       // completed Tasks list

	mu          sync.Mutex
	idGenerator func() int
}

type Task struct {
	TaskID     int
	AttemptID  int
	TaskType   TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Status     TaskStatus
	ExpiredAt  time.Time
}

type TaskStatus int

const (
	Ready TaskStatus = iota
	Running
	Completed
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AcquireTask(args *ExampleArgs, taskDef *TaskDefinition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// check if there are any tasks in the map task list
	if c.mapTasks.Len() != 0 {
		// move the first task from the readyTasks list to the runningTasks map
		// and update the task status to Running
		// and set the expired time to 10 seconds from now
		task := c.mapTasks.Remove(c.mapTasks.Front()).(*Task)
		task.Status = Running
		task.AttemptID = c.idGenerator()
		// set the expired time to 10 seconds from now
		task.ExpiredAt = time.Now().Add(10 * time.Second)
		c.runningTasks[task.TaskID] = task
		// set the task definition
		taskDef.TaskID = task.TaskID
		taskDef.AttemptID = task.AttemptID
		taskDef.TaskType = task.TaskType
		taskDef.MapTask = task.MapTask
		taskDef.ReduceTask = task.ReduceTask
	} else {
		// check if there are any running map tasks
		var numOfRunningMapTasks int = 0
		for _, task := range c.runningTasks {
			if task.TaskType == MAP {
				numOfRunningMapTasks++
			}
		}
		if numOfRunningMapTasks == 0 {
			// if all the map tasks are completed, then we can start the reduce tasks
			if len(c.reduceTasks) != 0 {
				// select the first reduce task from the reduceTasks map
				for _, task := range c.reduceTasks {
					task.Status = Running
					task.AttemptID = c.idGenerator()
					// set the expired time to 10 seconds from now
					task.ExpiredAt = time.Now().Add(10 * time.Second)
					c.runningTasks[task.TaskID] = task
					// set the task definition
					taskDef.TaskID = task.TaskID
					taskDef.AttemptID = task.AttemptID
					taskDef.TaskType = task.TaskType
					taskDef.MapTask = task.MapTask
					taskDef.ReduceTask = task.ReduceTask
					delete(c.reduceTasks, task.ReduceTask.ReducerIndex)
					break
				}
			}
		}
		// if there are no task for map and reduce, then we can return Terminal type
		// Task to worker to ask it to exit
		if len(c.reduceTasks) == 0 && len(c.runningTasks) == 0 {
			taskDef.TaskType = TERMINAL
		}
	}
	return nil
}

func (c *Coordinator) SendTaskResult(taskResult *TaskResult, reply *ExampleReply) error {
	// may not need to do anything here
	c.mu.Lock()
	defer c.mu.Unlock()
	// update the task status to Completed
	task := c.runningTasks[taskResult.TaskID]
	// make sure the attampt id is the same
	if task != nil && task.AttemptID == taskResult.AttemptID {
		task.Status = Completed
		c.completeTasks = append(c.completeTasks, task)
		delete(c.runningTasks, taskResult.TaskID)

		if task.TaskType == MAP {
			// generate reduce task and add it to the reduceTasks map
			c.mergeReduceTaskForReducer(taskResult)
		}
	}
	return nil
}

func (c *Coordinator) mergeReduceTaskForReducer(mapTaskResult *TaskResult) error {
	for idx, file := range mapTaskResult.FilePaths {
		if _, ok := c.reduceTasks[idx]; !ok {
			c.reduceTasks[idx] = &Task{
				TaskID:   c.idGenerator(),
				TaskType: REDUCE,
				ReduceTask: &ReduceTask{
					ReducerIndex:       idx,
					IntermedicateFiles: []string{file},
				},
				Status: Ready,
			}
		} else {
			c.reduceTasks[idx].ReduceTask.IntermedicateFiles = append(c.reduceTasks[idx].ReduceTask.IntermedicateFiles, file)
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.checkingExpiredTasks()
}

func (c *Coordinator) checkingExpiredTasks() {
	for {
		c.mu.Lock()
		for _, task := range c.runningTasks {
			if time.Now().After(task.ExpiredAt) {
				task.Status = Ready
				if task.TaskType == MAP {
					c.mapTasks.PushBack(task)
				} else {
					c.reduceTasks[task.ReduceTask.ReducerIndex] = task
				}
				delete(c.runningTasks, task.TaskID)
			}
		}
		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.mapTasks.Len() == 0 && len(c.reduceTasks) == 0 && len(c.runningTasks) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:      list.New(),
		reduceTasks:   make(map[int]*Task),
		runningTasks:  make(map[int]*Task),
		completeTasks: []*Task{},
		idGenerator:   idGenerator(),
	}
	// Your code here.
	for _, file := range files {
		c.mapTasks.PushBack(&Task{
			TaskID:   c.idGenerator(),
			TaskType: MAP,
			MapTask: &MapTask{
				FilePath: file,
				NReduce:  nReduce,
			},
			Status: Ready,
		})
	}
	c.server()
	return &c
}

func idGenerator() func() int {
	var mu sync.Mutex
	id := 0
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		id++
		return id
	}
}
