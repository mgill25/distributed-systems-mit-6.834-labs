package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

// BUG: reduce tasks are starting before map tasks have had a chance to finish
// Problem hint: time.Sleep or sync.Cond

type Task struct {
	fileName string
	status   string
}

// Global Data Structure
type Master struct {
	mux             sync.Mutex // mutex for concurrency control
	map_tasks       []Task
	reduce_tasks    []Task
	map_done        bool
	reduce_done     bool
	map_assigned    bool
	reduce_assigned bool
	map_counter     int
	reduce_counter  int
	nReduce         int
	numInputFiles   int
}

func (m *Master) launchMonitor(taskType string, taskId int) {
	time.Sleep(10 * time.Second)
	m.mux.Lock()
	if taskType == "map" {
		if m.map_tasks[taskId].status != "done" {
			log.Println("re-enqueuing a map task", taskId)
			m.map_tasks[taskId].status = "awaiting"
			m.map_assigned = false
			m.map_done = false
			m.map_counter--
		}
	} else if taskType == "reduce" {
		if m.reduce_tasks[taskId].status != "done" {
			log.Println("re-enqueuing a reduce task", taskId)
			m.reduce_tasks[taskId].status = "awaiting"
			m.reduce_assigned = false
			m.reduce_done = false
			m.reduce_counter--
		}
	}
	m.mux.Unlock()
}

func (m *Master) GetTask(req *TaskRequest, res *TaskResponse) error {
	// time.Sleep(2 * time.Second)
	m.mux.Lock()
	if m.map_assigned && !m.map_done {
		log.Println("map not done yet...")
		m.sendWaitSignal(req, res)
	}

	if !m.map_assigned && !m.map_done {
		m.assignToWorker("map", req, res)
		go m.launchMonitor("map", m.map_counter)
		if m.map_counter == m.numInputFiles-1 {
			m.map_assigned = true
		} else {
			m.map_counter++
		}
		m.mux.Unlock()
		return nil
	} else if m.map_done && !m.reduce_assigned && !m.reduce_done {
		m.assignToWorker("reduce", req, res)
		go m.launchMonitor("reduce", m.reduce_counter)
		if m.reduce_counter == m.nReduce-1 {
			m.reduce_assigned = true
		} else {
			m.reduce_counter++
		}
		m.mux.Unlock()
		return nil
	}

	if m.map_done && m.reduce_done {
		m.sendQuitSignal(req, res)
	} else {
		m.sendWaitSignal(req, res)
	}
	m.mux.Unlock()
	return nil
}

// Assign a task to the requesting worker
func (m *Master) assignToWorker(taskType string, req *TaskRequest, res *TaskResponse) {
	log.Println(fmt.Sprintf("[WORKER], Assigning taskType = %v", taskType))
	// Create RPC response
	// And then change the internal state to reflect current status
	res.TaskType = taskType
	res.NReduce = m.nReduce
	if taskType == "map" {
		res.FileName = m.map_tasks[m.map_counter].fileName
		res.TaskId = m.map_counter
		res.Msg = "map_task"
		m.map_tasks[m.map_counter].status = "assigned"
	} else if taskType == "reduce" {
		// no filename needed for reduce
		res.TaskId = m.reduce_counter
		res.Msg = "reduce_task"
		res.NumInputFiles = m.numInputFiles
		m.reduce_tasks[m.reduce_counter].status = "assigned"
	}
}

func (m *Master) sendQuitSignal(req *TaskRequest, res *TaskResponse) {
	res.Msg = "quit"
}

func (m *Master) sendWaitSignal(req *TaskRequest, res *TaskResponse) {
	res.Msg = "wait"
}

// are all map tasks done?
func (m *Master) isMapDone() bool {
	all_map_done := true
	for i := range m.map_tasks {
		if m.map_tasks[i].status == "done" {
			all_map_done = all_map_done && true
		} else {
			all_map_done = all_map_done && false
		}
	}
	return all_map_done
}

// are all reduce tasks done?
func (m *Master) isReduceDone() bool {
	all_reduce_done := true
	for i := range m.reduce_tasks {
		if m.reduce_tasks[i].status == "done" {
			all_reduce_done = all_reduce_done && true
		} else {
			all_reduce_done = all_reduce_done && false
		}
	}
	return all_reduce_done
}

// Mark this particular file as done
func (m *Master) MarkDone(req *DoneReq, res *DoneRes) error {
	m.mux.Lock()
	taskType := req.TaskType
	if taskType == "map" && !m.map_done {
		log.Println(fmt.Sprintf("[MASTER] marking map task %v as done", req.TaskId))
		m.map_tasks[req.TaskId].status = "done"
		m.map_done = m.isMapDone()
		if m.map_done {
			log.Println("[MASTER] >>>>>>>>>>>>>>>>>> all map done")
		}
	}
	if taskType == "reduce" && !m.reduce_done {
		log.Println(fmt.Sprintf("[MASTER] marking reduce task %v as done", req.TaskId))
		m.reduce_tasks[req.TaskId].status = "done"
		m.reduce_done = m.isReduceDone()
		if m.reduce_done {
			log.Println("[MASTER] >>>>>>>>>>>>>>>>>> all reduce done")
		}
	}
	res.Ok = true
	m.mux.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Starting server...")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mux.Lock()
	if m.map_done && m.reduce_done {
		ret = true
	}
	m.mux.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	log.Println("Making a master...")
	log.Println("processing", len(files), "input files...")
	log.Println("will launch", nReduce, "reduce workers")

	m := Master{}

	m.map_tasks = []Task{}
	m.reduce_tasks = []Task{}
	m.map_done = false
	m.reduce_done = false
	m.map_assigned = false
	m.reduce_assigned = false
	m.map_counter = 0
	m.reduce_counter = 0
	m.nReduce = nReduce
	m.numInputFiles = len(files)

	// initialize all map tasks
	for _, file := range files {
		mapTask := Task{
			fileName: file,
			status:   "awaiting",
		}
		m.map_tasks = append(m.map_tasks, mapTask)
	}

	// initialize all reduce tasks
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{
			status: "awaiting",
		}
		m.reduce_tasks = append(m.reduce_tasks, reduceTask)
	}

	m.server()
	return &m
}
