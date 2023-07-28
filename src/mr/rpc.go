package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
// rpc requests to ask coordinator for a job
type ReqJobArgs struct{}
type ReqJobReply struct {
	Ttype     string // task type , "map" or "reduce" or "exit" or "busy"
	File      string // file to be processed, if a map task
	MapCnt    int    // total number of map tasks (number of files)
	ReduceCnt int    // total number of reduce tasks (number of buckets)
	Id        int    // if a map task, the id of the input file; if a reduce task, the id of the bucket
}

// rpc requests to ack a finished job
type AckJobArgs struct {
	Ttype string // task type, "map" or "reduce"
	Tid   int    // if a map task, the id of the input file;  if a reduce task, the id of the bucket
}
type AckJobReply struct{} //empty reply for ack

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
