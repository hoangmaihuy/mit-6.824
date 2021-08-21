package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	go MapWorker(mapf)
	go ReduceWorker(reducef)
	select {}
}

func MapWorker(mapf func(string, string) []KeyValue) {
	for {
		inputFile, mapNumber, nReduce := requestMapTask()
		if mapNumber == -1 {
			time.Sleep(time.Second)
		} else {
			file, err := os.Open(inputFile)
			if err != nil {
				log.Fatalf("cannot open input file: %v", inputFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read input file: %v", inputFile)
			}
			// do the map
			kva := mapf(inputFile, string(content))
			// split map result into NReduce bucket and send to reducer
			intermediates := make([][]KeyValue, nReduce)
			for _, elem := range kva {
				i := ihash(elem.Key) % nReduce
				intermediates[i] = append(intermediates[i], elem)
			}
			// write intermediates and notify coordinator
			for i := 0; i < nReduce; i++ {
				go func(reduceNumber int) {
					iname := fmt.Sprintf("mr-%v-%v", mapNumber, reduceNumber)
					ifile, _ := os.Create(iname)
					defer ifile.Close()
					enc := json.NewEncoder(ifile)
					for _, kv := range intermediates[reduceNumber] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot encode intermediate %v", kv)
						}
					}
					completeMapTask(mapNumber, reduceNumber, iname)
				}(i)
			}
		}
	}
}

func ReduceWorker(reducef func(string, []string) string) {
	for {
		reduceNumber, intermediateFiles := requestReduceTask()
		if reduceNumber == -1 {
			time.Sleep(time.Second)
		} else {
			var intermediate []KeyValue
			// read all intermediate files and combine to one
			for _, iname := range intermediateFiles {
				ifile, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open intermediate %v", iname)
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break // EOF
					}
					intermediate = append(intermediate, kv)
				}
				ifile.Close()
			}
			// sort values, pass to reducef and write to output file
			// this part is stolen from mrsequential.go

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reduceNumber)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-{reduceNumber}
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			// finally, notify coordinator
			completeReduceTask(reduceNumber, oname)
		}
	}
}
// the RPC argument and reply types are defined in rpc.go.

func requestMapTask() (string, int, int) {
	args := RequestMapTaskArgs{}
	reply := RequestMapTaskReply{}

	DPrintf("RequestMapTask: args = %v", args)
	call("Coordinator.RequestMapTask", &args, &reply)
	DPrintf("RequestMapTask: reply = %v", reply)
	return reply.InputFile, reply.MapNumber, reply.NReduce
}

func completeMapTask(mapNumber int, reduceNumber int, intermediateFile string) {
	args := CompleteMapTaskArgs{mapNumber, reduceNumber, intermediateFile}
	reply := CompleteReduceTaskReply{}
	DPrintf("CompleteMapTask: args = %v", args)
	call("Coordinator.CompleteMapTask", &args, &reply)
	DPrintf("CompleteMapTask: reply = %v", reply)
}

func requestReduceTask() (int, []string) {
	args := RequestReduceTaskArgs{}
	reply := RequestReduceTaskReply{}

	DPrintf("RequestReduceTask: args = %v", args)
	call("Coordinator.RequestReduceTask", &args, &reply)
	DPrintf("RequestReduceTask: reply = %v", reply)
	return reply.ReduceNumber, reply.IntermediateFiles
}

func completeReduceTask(reduceNumber int, outputFile string) {
	args := CompleteReduceTaskArgs{reduceNumber, outputFile}
	reply := CompleteReduceTaskReply{}

	DPrintf("CompleteReduceTask: args = %v", args)
	call("Coordinator.CompleteReduceTask", &args, &reply)
	DPrintf("CompleteReduceTask: reply = %v", reply)
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
