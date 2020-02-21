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
	for {
		log.Println("[WORKER] Requesting new task...")
		res := GetNewTask()
		if res.Msg == "quit" {
			log.Println("[WORKER] Quitting...")
			break
		} else if res.Msg == "map_task" {
			HandleMap(&res, mapf)
		} else if res.Msg == "reduce_task" {
			HandleReduce(&res, reducef)
		} else {
			log.Println("Do nothing. Call again")
		}
		time.Sleep(1 * time.Second)
	}
}

func GetNewTask() TaskResponse {
	req := TaskRequest{}
	res := TaskResponse{}
	call("Master.GetTask", &req, &res)
	return res
}

func HandleMap(res *TaskResponse, mapf mapperFunc) {
	if res.TaskType == "map" {
		log.Println("[WORKER] Executing the map task", res.TaskId)
		err := ExecuteMapTask(mapf, res.FileName, res.TaskId, res.NReduce)
		if err != nil {
			log.Println("Error during map execution = ", err)
		}
		mreq := DoneReq{res.FileName, res.TaskId, "map"}
		mres := DoneRes{}
		// log.Println("[WORKER] RPC to mark map as done")
		call("Master.MarkDone", &mreq, &mres)
	}
}

func HandleReduce(res *TaskResponse, reducef reduceFunc) {
	if res.TaskType == "reduce" {
		log.Println("[WORKER] Executing the reduce task", res.TaskId)
		err := ExecuteReduceTask(reducef, res.TaskId, res.NumInputFiles)
		if err != nil {
			log.Println("Error during reduce execution = ", err)
		}
		mreq := DoneReq{res.FileName, res.TaskId, "reduce"}
		mres := DoneRes{}
		// log.Println("[WORKER] RPC to mark reduce as done")
		call("Master.MarkDone", &mreq, &mres)
	}
}

// Map executor
func ExecuteMapTask(mapf mapperFunc, fileName string, mapTaskId int, nReduce int) error {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf(fmt.Sprintf("cannot open %v", fileName))
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf(fmt.Sprintf("cannot open %v", fileName))
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
		log.Fatalf(fmt.Sprintf("cannot open %v", fileName))
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
	// log.Println("reduce = ", reduceTaskId, " total = ", totalMapTasks)
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
