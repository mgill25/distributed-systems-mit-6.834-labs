package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "errors"

type Task struct {
	id       int
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
	nReduce         int
	numInputFiles   int
}

func (m *Master) getTaskById(taskType string, taskId int) (Task, error) {
	var arr []Task
	if taskType == "map" {
		arr = m.map_tasks
	} else {
		arr = m.reduce_tasks
	}
	for _, t := range arr {
		if t.id == taskId {
			return t, nil
		}
	}
	return Task{}, errors.New("No task by given id found")
}

func (m *Master) launchMonitor(taskType string, task Task) {
	taskId := task.id
	time.Sleep(10 * time.Second)
	m.mux.Lock()
	if taskType == "map" {
		t, err := m.getTaskById("map", taskId)
		if err != nil {
			log.Fatal(err)
		}
		if t.status != "done" {
			for i, tt := range m.map_tasks {
				if t.id == tt.id {
					m.map_tasks[i].status = "awaiting"
				}
			}
			m.map_assigned = false
			m.map_done = false
			log.Println("re-enqueued a map task", taskId, m.map_tasks)
			log.Println("[MONITOR] Map all assigned:", m.map_assigned)
		}
	} else if taskType == "reduce" {
		t, err := m.getTaskById("reduce", taskId)
		if err != nil {
			log.Fatal(err)
		}
		if t.status != "done" {
			for i, tt := range m.reduce_tasks {
				if t.id == tt.id {
					m.reduce_tasks[i].status = "awaiting"
				}
			}
			m.reduce_assigned = false
			m.reduce_done = false
			log.Println("re-enqueued a reduce task", taskId, m.reduce_tasks)
			log.Println("[MONITOR] Reduce all assigned:", m.reduce_assigned)
		}
	}
	m.mux.Unlock()
}

// Iterate over the entire state and return the next
// task that the workers can process
func (m *Master) pickNextAwaitingTask(taskType string) (Task, bool) {
	var arr []Task
	if taskType == "map" {
		arr = m.map_tasks
	} else if taskType == "reduce" {
		arr = m.reduce_tasks
	}

	for i, t := range arr {
		if t.status == "done" || t.status == "assigned" {
			// task has either been done already or is currently in progress
			continue
		}
		// otherwise, this task can be picked.
		arr[i].status = "assigned"
		if i == len(arr)-1 {
			// reached the end of array. all tasks assigned at least once.
			return t, true
		}
		return t, false
	}
	return Task{}, false
}

// RPC Get a new task for a worker
// MUTATION:
//	- map_assigned, reduce_assigned flags
func (m *Master) GetTask(req *TaskRequest, res *TaskResponse) error {
	m.mux.Lock()

	// Basic flag pre-condition checks
	if m.isMapDone() && m.isReduceDone() {
		m.sendQuitSignal(req, res)
	}

	if !m.map_assigned && !m.isMapDone() {
		nextTask, allAssigned := m.pickNextAwaitingTask("map")
		if allAssigned {
			m.map_assigned = true
		}
		m.assignToWorker("map", nextTask, req, res)
		go m.launchMonitor("map", nextTask)
		m.mux.Unlock()
		return nil
	} else if m.isMapDone() && !m.reduce_assigned && !m.isReduceDone() {
		nextTask, allAssigned := m.pickNextAwaitingTask("reduce")
		if allAssigned {
			m.reduce_assigned = true
		}
		m.assignToWorker("reduce", nextTask, req, res)
		go m.launchMonitor("reduce", nextTask)
		m.mux.Unlock()
		return nil
	} else {
		m.sendWaitSignal(req, res)
	}

	m.mux.Unlock()
	return nil
}

// RPC helper: Assign a task to the requesting worker
// Mutation: RPC response object
func (m *Master) assignToWorker(taskType string, task Task, req *TaskRequest, res *TaskResponse) {
	// Create RPC response
	// And then change the internal state to reflect current status
	res.TaskType = taskType
	res.NReduce = m.nReduce
	if taskType == "map" {
		res.FileName = task.fileName
		res.TaskId = task.id
		res.Msg = "map_task"
	} else if taskType == "reduce" {
		res.TaskId = task.id
		res.Msg = "reduce_task"
		res.NumInputFiles = m.numInputFiles
	}
}

// RPC MUTATION msg
func (m *Master) sendQuitSignal(req *TaskRequest, res *TaskResponse) {
	res.Msg = "quit"
}

// RPC MUTATION msg
func (m *Master) sendWaitSignal(req *TaskRequest, res *TaskResponse) {
	res.Msg = "wait"
}

// QUERY: Are all map tasks done?
func (m *Master) isMapDone() bool {
	for i := range m.map_tasks {
		if m.map_tasks[i].status != "done" {
			return false
		}
	}
	return true
}

// QUERY: Are all reduce tasks done?
func (m *Master) isReduceDone() bool {
	for i := range m.reduce_tasks {
		if m.reduce_tasks[i].status != "done" {
			return false
		}
	}
	return true
}

// MUTATION: Mark a single map or reduce task as done.
//		- task.status = "done" if matches the taskId
func (m *Master) MarkTaskDone(taskArray []Task, taskId int) {
	for i, t := range taskArray {
		if t.id == taskId {
			taskArray[i].status = "done"
		}
	}
}

// RPC: Receives a task Id from worker and marks that task done.
// MUTATION:
//		- map_done, reduce_done flags
//		- task.status (from an inner function call)
func (m *Master) MarkDone(req *DoneReq, res *DoneRes) error {
	m.mux.Lock()
	taskType := req.TaskType
	if taskType == "map" && !m.map_done {
		m.MarkTaskDone(m.map_tasks, req.TaskId)
		m.map_done = m.isMapDone()
	} else if taskType == "reduce" && !m.reduce_done {
		m.MarkTaskDone(m.reduce_tasks, req.TaskId)
		m.reduce_done = m.isReduceDone()
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
	m.nReduce = nReduce
	m.numInputFiles = len(files)

	// initialize all map tasks
	for i, file := range files {
		mapTask := Task{
			id:       i,
			fileName: file,
			status:   "awaiting",
		}
		m.map_tasks = append(m.map_tasks, mapTask)
	}

	// initialize all reduce tasks
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{
			id:     i,
			status: "awaiting",
		}
		m.reduce_tasks = append(m.reduce_tasks, reduceTask)
	}

	m.server()
	return &m
}
