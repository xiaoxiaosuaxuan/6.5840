package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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
	for {
		args := ReqJobArgs{}
		reply := ReqJobReply{}
		err := call("Coordinator.ReqJob", args, &reply)
		if err != nil || reply.Ttype == "exit" {
			if err != nil {
				log.Fatalf("call RPC ReqJob failed with %v!", err)
			}
			break
		}
		log.Printf("get job with type %v", reply.Ttype)
		if reply.Ttype == "busy" {
			time.Sleep(time.Second)
			continue
		}
		if reply.Ttype == "map" {
			handleMap(reply.File, reply.Id, reply.ReduceCnt, mapf)
		} else if reply.Ttype == "reduce" {

		} else {
			log.Fatalf("unsupported task type %v !", reply.Ttype)
		}
	}
}

func handleMap(fname string, mapId int, reduceCnt int,
	mapf func(string, string) []KeyValue) {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("cannot open %v", fname)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fname)
	}
	file.Close()
	kv_all := mapf(fname, string(content))
	kv_buckets := make([][]KeyValue, reduceCnt)
	for i := range kv_all {
		rid := ihash(kv_all[i].Key) % reduceCnt
		kv_buckets[rid] = append(kv_buckets[rid], kv_all[i])
	}
	for rid := 0; rid < reduceCnt; rid++ {
		midFname := fmt.Sprintf("mr-%d-%d.json", mapId, rid)
		midFile, err := os.Create(midFname)
		if err != nil {
			log.Fatal(err)
		}
		kv_json, err := json.Marshal(kv_buckets[rid])
		if err != nil {
			log.Fatal(err)
		}
		_, err = midFile.Write(kv_json)
		if err != nil {
			log.Fatal(err)
		}
	}
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
	err := call("Coordinator.Example", &args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}
	return nil
}
