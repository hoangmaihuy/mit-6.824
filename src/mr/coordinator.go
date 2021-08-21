package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

const (
	Idle TaskState = iota
	Ready TaskState = iota
	InProgress TaskState = iota
	Completed TaskState = iota
)

type MapTask struct {
	mutex sync.Mutex
	inputFile string
	intermediateFiles []string
	state TaskState
}

type ReduceTask struct {
	mutex sync.Mutex
	intermediateFiles []string
	outputFile string
	state TaskState
}

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nReduce     int
}

//
// RPC handlers
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) RequestMapTask(args *RequestMapTaskArgs, reply *RequestMapTaskReply) error {
	log.Printf("RequestMapTask: received")
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		task.mutex.Lock()
		if task.state == Ready {
			reply.InputFile = task.inputFile
			reply.MapNumber = i
			reply.NReduce = c.nReduce
			task.state = InProgress
			task.mutex.Unlock()
			log.Printf("RequestMapTask: returned %v", reply)
			return nil
		}
		task.mutex.Unlock()
	}

	reply.MapNumber = -1
	log.Printf("RequestMapTask: no ready task")
	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	log.Printf("CompleteMapTask: args = %v", args)
	// append intermediate file to map task
	if i := args.MapNumber; i < len(c.mapTasks) {
		mapTask := &c.mapTasks[i]
		mapTask.mutex.Lock()
		mapTask.intermediateFiles = append(mapTask.intermediateFiles, args.IntermediateFile)
		if len(mapTask.intermediateFiles) == c.nReduce {
			mapTask.state = Completed
		}
		mapTask.mutex.Unlock()

	}
	// append intermediate file to reduce task
	if j := args.ReduceNumber; j < len(c.reduceTasks) {
		reduceTask := &c.reduceTasks[j]
		reduceTask.mutex.Lock()
		reduceTask.intermediateFiles = append(reduceTask.intermediateFiles, args.IntermediateFile)
		// all intermediate files are collected
		if len(reduceTask.intermediateFiles) == len(c.mapTasks) {
			reduceTask.state = Ready
		}
		reduceTask.mutex.Unlock()
	}
	log.Printf("CompleteMapTask: returned")
	return nil
}

func (c *Coordinator) RequestReduceTask(args *RequestReduceTaskArgs, reply *RequestReduceTaskReply) error {
	log.Printf("RequestReduceTask: args = %v", args)
	for j := range c.reduceTasks {
		task := &c.reduceTasks[j]
		task.mutex.Lock()
		if task.state == Ready {
			reply.ReduceNumber = j
			reply.IntermediateFiles = task.intermediateFiles
			task.state = InProgress
			task.mutex.Unlock()
			log.Printf("RequestReduceTask: reply = %v", reply)
			return nil
		}
		task.mutex.Unlock()
	}
	reply.ReduceNumber = -1
	log.Printf("RequestReduceTask: no ready task")
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	log.Printf("CompleteReduceTask: args = %v", args)
	if j := args.ReduceNumber; j < len(c.reduceTasks) {
		task := &c.reduceTasks[j]
		task.mutex.Lock()
		task.outputFile = args.OutputFile
		task.state = Completed
		task.mutex.Unlock()
	}
	log.Printf("CompleteReduceTask: returned")
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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)

	for i, file := range files {
		c.mapTasks[i].mutex = sync.Mutex{}
		c.mapTasks[i].inputFile = file
		c.mapTasks[i].state = Ready
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i].mutex = sync.Mutex{}
		c.reduceTasks[i].state = Idle
	}

	c.server()
	return &c
}
