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
type RequestMapTaskArgs struct{}

type RequestMapTaskReply struct {
	InputFile string
	MapNumber int
	NReduce   int
}

type CompleteMapTaskArgs struct {
	MapNumber        int
	ReduceNumber     int
	IntermediateFile string
}

type CompleteMapTaskReply struct{}

type RequestReduceTaskArgs struct{}

type RequestReduceTaskReply struct {
	ReduceNumber      int
	IntermediateFiles []string
}

type CompleteReduceTaskArgs struct {
	ReduceNumber int
	OutputFile   string
}

type CompleteReduceTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
