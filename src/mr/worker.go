package mr

import "os"
import "io/ioutil"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

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

type mapperFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf mapperFunc, reducef reduceFunc) {
	// Repeatedly call the Master via RPC and ask for new tasks
	for {
		// what happens when a worker has successfully finished a task?
		// it should ping the master and ask for any next available task.
		rv := CallMaster(mapf, reducef)
		if rv == "close" {
			break
		} else if rv == "retry" {
			log.Println("Retry after a second...")
			time.Sleep(1 * time.Second)
			continue
		}
		time.Sleep(1 * time.Second)
	}
}

// This function calls the Master server via RPC
func CallMaster(mapf mapperFunc, reducef reduceFunc) string {
	// Worker has a few jobs that happen within this
	// 1. RPC call to master. Master replies with an input file name and a task number
	// 2. Tasks can also be of 2 types: Map Task or Reduce Task.
	// 3. Worker should process the task as per the spec (different for Map and Reduce)
	// 4. After executing the task, worker should signal to master
	req := TaskRequest{}
	res := TaskResponse{}
	call("Master.GetTask", &req, &res)
	if res.TaskType == "wait" {
		// master is asking worker to wait
		return "retry"
	}

	// log.Println(fmt.Sprintf("Got a %v Task[%v], Filename = %v\n", res.TaskType, res.TaskId, res.FileName))

	if res.TaskType == "MapTask" {
		log.Println("Executing the map task", res.TaskId)
		err := ExecuteMapTask(mapf, res.FileName, res.TaskId, res.NReduce)
		if err != nil {
			log.Println("Error during map execution = ", err)
		}
		mreq := DoneReq{res.FileName, res.TaskId, "Map"}
		mres := DoneRes{}
		call("Master.MarkDone", &mreq, &mres)
	} else if res.TaskType == "ReduceTask" {
		log.Println("Executing the reduce task", res.TaskId)
		err := ExecuteReduceTask(reducef, res.TaskId, res.MDoneTasks)
		if err != nil {
			log.Println("Error during reduce execution = ", err)
		}
		mreq := DoneReq{res.FileName, res.TaskId, "Reduce"}
		mres := DoneRes{}
		call("Master.MarkDone", &mreq, &mres)
	} else if res.TaskType == "CloseWorker" {
		return "close"
	}
	return ""
}

// Map executor
func ExecuteMapTask(mapf mapperFunc, fileName string, mapTaskId int, nReduce int) error {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return err
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	// split the intermediate array by key
	// into 1 intermediate file per reduce job
	i := 0
	batch := make(map[string][]KeyValue)
	for i < len(intermediate) {
		// A reasonable naming convention for intermediate files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.
		reduceTaskId := ihash(intermediate[i].Key) % nReduce
		iFileName := fmt.Sprintf("mr-%v-%v", mapTaskId, reduceTaskId)
		arr, _ := batch[iFileName]
		arr = append(arr, intermediate[i])
		batch[iFileName] = arr
		i++
	}
	for fileName, batchArray := range batch {
		writeIntermediateResult(fileName, batchArray)
	}
	return nil
}

// json encoded intermediate result into mapper files
func writeIntermediateResult(fileName string, intermediateResult []KeyValue) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range intermediateResult {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Got error = ", err)
		}
	}
}

// Reduce executor
func ExecuteReduceTask(reducef reduceFunc, reduceTaskId int, totalMapTasks int) error {
	var intermediate []KeyValue
	for i := 0; i < totalMapTasks; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", i, reduceTaskId)
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceTaskId)

	ofile, err := ioutil.TempFile(".", "temp-")
	if err != nil {
		log.Fatal("Got error = ", err)
	}
	defer os.Remove(ofile.Name())
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
	// Atomic rename operation to avoid seeing partial data
	os.Rename(ofile.Name(), oname)
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
