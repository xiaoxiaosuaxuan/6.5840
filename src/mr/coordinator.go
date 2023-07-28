package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	lock           sync.Mutex // 锁，用来保护并发rpc请求时的任务状态
	files          []string   // 输入文件的切片
	mTaskStats     []int      // map task 状态； 0: 未分配  1: 已分配    2:已完成
	rTaskStats     []int      // reduce task 状态；0:未分配  1:已分配   2:已完成
	finishedMap    int        // 已完成 map task 的数量
	finishedReduce int        // 已完成 reduce task 的数量
	reduceCnt      int        // reduce task 的数量
}

// Your code here -- RPC handlers for the worker to call.
// rpc call for workers to ask for job
func (c *Coordinator) ReqJob(args ReqJobArgs, reply *ReqJobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.MapCnt = len(c.files)
	reply.ReduceCnt = c.reduceCnt
	reply.Ttype = ""
	if c.finishedMap == len(c.files) { // give reduce or exit task
		if c.finishedReduce == c.reduceCnt {
			reply.Ttype = "exit"
		} else {
			for idx, stat := range c.rTaskStats {
				if stat == 0 {
					c.rTaskStats[idx] = 1
					reply.Ttype = "reduce"
					reply.Id = idx
					break
				}
			}
		}
	} else { // give map task
		for idx, file := range c.files {
			if c.mTaskStats[idx] == 0 {
				c.mTaskStats[idx] = 1
				reply.Ttype = "map"
				reply.File = file
				reply.Id = idx
				break
			}
		}
	}
	if reply.Ttype == "" {
		reply.Ttype = "busy"
	}
	return nil
}

// rpc call for workers to ack finished job
func (c *Coordinator) AckJob(args *AckJobArgs, reply *AckJobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.Ttype == "map" {
		if c.mTaskStats[args.Tid] == 1 {
			c.mTaskStats[args.Tid] = 2
			c.finishedMap += 1
		}
	} else if args.Ttype == "reduce" {
		if c.rTaskStats[args.Tid] == 1 {
			c.rTaskStats[args.Tid] = 2
			c.finishedReduce += 1
		}
	} else {
		return fmt.Errorf("unsupported ack job with type %v", args.Ttype)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		lock:           sync.Mutex{},
		files:          files,
		mTaskStats:     make([]int, len(files)),
		rTaskStats:     make([]int, nReduce),
		finishedMap:    0,
		finishedReduce: 0,
		reduceCnt:      nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
