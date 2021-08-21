package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TaskTimeout = 10

type TaskState int

const (
	Idle       TaskState = iota
	Ready      TaskState = iota
	InProgress TaskState = iota
	Completed  TaskState = iota
)

type MapTask struct {
	mutex             sync.Mutex
	inputFile         string
	intermediateFiles []string
	state             TaskState
	elapsed           int
}

type ReduceTask struct {
	mutex             sync.Mutex
	intermediateFiles []string
	outputFile        string
	state             TaskState
	elapsed           int
}

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nMap        int
	nReduce     int
}

//
// RPC handlers
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) RequestMapTask(args *RequestMapTaskArgs, reply *RequestMapTaskReply) error {
	DPrintf("RequestMapTask: received")
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		task.mutex.Lock()
		if task.state == Ready {
			reply.InputFile = task.inputFile
			reply.MapNumber = i
			reply.NReduce = c.nReduce
			task.state = InProgress
			task.elapsed = 0
			task.mutex.Unlock()
			DPrintf("RequestMapTask: returned %v", reply)
			return nil
		}
		task.mutex.Unlock()
	}

	reply.MapNumber = -1
	DPrintf("RequestMapTask: no ready task")
	return nil
}

// empty string means the file is not collected
func isAllCollected(files []string) bool {
	for _, file := range files {
		if file == "" {
			return false
		}
	}
	return true
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	DPrintf("CompleteMapTask: args = %v", args)
	// append intermediate file to map task
	i := args.MapNumber
	j := args.ReduceNumber
	if i < len(c.mapTasks) {
		mapTask := &c.mapTasks[i]
		mapTask.mutex.Lock()
		mapTask.intermediateFiles[j] = args.IntermediateFile
		if isAllCollected(c.mapTasks[i].intermediateFiles) {
			mapTask.state = Completed
		}
		mapTask.mutex.Unlock()

	}
	// append intermediate file to reduce task
	if j < len(c.reduceTasks) {
		reduceTask := &c.reduceTasks[j]
		reduceTask.mutex.Lock()
		reduceTask.intermediateFiles[i] = args.IntermediateFile
		// all intermediate files are collected
		if isAllCollected(reduceTask.intermediateFiles) {
			reduceTask.state = Ready
		}
		reduceTask.mutex.Unlock()
	}
	DPrintf("CompleteMapTask: returned")
	return nil
}

func (c *Coordinator) RequestReduceTask(args *RequestReduceTaskArgs, reply *RequestReduceTaskReply) error {
	DPrintf("RequestReduceTask: args = %v", args)
	for j := range c.reduceTasks {
		task := &c.reduceTasks[j]
		task.mutex.Lock()
		if task.state == Ready {
			reply.ReduceNumber = j
			reply.IntermediateFiles = task.intermediateFiles
			task.state = InProgress
			task.elapsed = 0
			task.mutex.Unlock()
			DPrintf("RequestReduceTask: reply = %v", reply)
			return nil
		}
		task.mutex.Unlock()
	}
	reply.ReduceNumber = -1
	DPrintf("RequestReduceTask: no ready task")
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	DPrintf("CompleteReduceTask: args = %v", args)
	if j := args.ReduceNumber; j < len(c.reduceTasks) {
		task := &c.reduceTasks[j]
		task.mutex.Lock()
		task.outputFile = args.OutputFile
		task.state = Completed
		task.mutex.Unlock()
	}
	DPrintf("CompleteReduceTask: returned")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	checkMapTasks(c)
	checkReduceTasks(c)
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		task.mutex.Lock()
		if task.state != Completed {
			task.mutex.Unlock()
			return false
		}
		task.mutex.Unlock()
	}

	return true
}

func checkMapTasks(c *Coordinator) {
	for i := range c.mapTasks {
		mapTask := &c.mapTasks[i]
		mapTask.mutex.Lock()
		if mapTask.state == InProgress {
			if mapTask.elapsed++; mapTask.elapsed > TaskTimeout {
				// worker probably crashed, mark as Ready to schedule to other workers
				mapTask.state = Ready
			}
		}
		mapTask.mutex.Unlock()
	}
}

func checkReduceTasks(c *Coordinator) {
	for j := range c.reduceTasks {
		reduceTask := &c.reduceTasks[j]
		reduceTask.mutex.Lock()
		if reduceTask.state == InProgress {
			if reduceTask.elapsed++; reduceTask.elapsed > TaskTimeout {
				reduceTask.state = Ready
			}
		}
		reduceTask.mutex.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)

	for i, file := range files {
		c.mapTasks[i].mutex = sync.Mutex{}
		c.mapTasks[i].inputFile = file
		c.mapTasks[i].state = Ready
		c.mapTasks[i].intermediateFiles = make([]string, c.nReduce)
	}

	for j := 0; j < nReduce; j++ {
		c.reduceTasks[j].mutex = sync.Mutex{}
		c.reduceTasks[j].state = Idle
		c.reduceTasks[j].intermediateFiles = make([]string, c.nMap)
	}

	c.server()
	return &c
}
