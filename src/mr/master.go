package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type MapFileState struct {
	FileName string
	Status   string 				// possible values: awaiting, assigned, done
	TaskId   int
}

// Global Data Structure
type Master struct {
	FileStates []MapFileState 		// name and status of input files (for map tasks)
	NReduce    int 					// number: total number of reduce tasks to launch
	MDoneTasks int 					// counter: total map tasks successfully done so far
	MAssigned  int                  // counter: map tasks assigned to workers so far
	RAssigned  int                  // counter: reduce tasks assigned to workes so far
	TaskInfo   map[int]string       // map of every task id with its status
	mux        sync.Mutex           // mutex for concurrency control

	// Note: `MAssigned` and `RAssigned` are used as task ids which get assigned to workers,
	// and are increased sequentially (assuming serial and not concurrent operations).
}

// Worker RPCs the Master on this endpoint to request for a new task
/**

The master should notice if a worker hasn't completed its task in a reasonable
amount of time (for this lab, use ten seconds), and give the same task to a
different worker.
	- TODO: How can we achieve this?
**/

func (m *Master) launchMonitor(taskId int, fileName string) {
	// lauch a monitor goroutine for this task
	time.Sleep(10 * time.Second) 	// wait 10 seconds
	m.mux.Lock()
	// if this task isn't marked as done after 10 seconds,
	// we need to go in our master data structure, mark the task as "awaiting"
	if m.TaskInfo[taskId] != "done" {
		for i, fileState := range m.FileStates {
			if fileState.FileName == fileName {
				fileState.Status = "awaiting"
				m.TaskInfo[taskId] = "awaiting"
				m.FileStates[i] = fileState
				m.MAssigned--
				// log.Println(fmt.Sprintf("task %v re-marked as awaiting. MAssigned = %v", taskId, m.MAssigned))
				break
			}
		}
	}
	// the next thing we need to do is assign this exact task (file) to 
	// another worker. How can we achieve that?
	m.mux.Unlock()
}

// well the worker is asking for the task, and it only asks for tasks that are marked not done.
// so it should automatically ask for the awaiting tasks (which were marked awaiting by the 
// monitoring thread!)
func (m *Master) GetTask(req *TaskRequest, res *TaskResponse) error {
	m.mux.Lock()
	if m.MDoneTasks < len(m.FileStates) && m.MAssigned == len(m.FileStates) {
		log.Println("all tasks have been assigned to workers!")
		log.Println("TaskInfo = ", m.TaskInfo)
		res.TaskType = "wait"
	}
	if m.MDoneTasks == len(m.FileStates) && m.RAssigned < m.NReduce {
		res.TaskType = "ReduceTask"
		res.MDoneTasks = m.MDoneTasks
		// keep track of all reduce tasks assigned
		res.TaskId = m.RAssigned
		go m.launchMonitor(res.TaskId, "")
		m.RAssigned++
		m.TaskInfo[res.TaskId] = "assigned"
	} else if m.MDoneTasks < len(m.FileStates) {
		item, fileStates := m.FileStates[0], m.FileStates[1:]
		item.Status = "assigned"
		fileStates = append(fileStates, item)
		m.FileStates = fileStates
		// Create the RPC response for the Map
		res.FileName = item.FileName
		res.TaskId  = m.MAssigned
		res.NReduce = m.NReduce
		res.TaskType = "MapTask"
		go m.launchMonitor(res.TaskId, res.FileName)
		m.MAssigned++
		m.TaskInfo[res.TaskId] = "assigned"
	} else {
		// Map and Reduce have finished. Close the worker gracefully
		res.TaskType = "CloseWorker"
	}
	m.mux.Unlock()
	return nil
}

// Mark this particular file as done
func (m *Master) MarkDone(req *DoneReq, res *DoneRes) error {
	m.mux.Lock()
	if req.TaskType == "Map" {
		for i, fileState := range m.FileStates {
			if fileState.Status == "assigned" && m.MDoneTasks < len(m.FileStates) {
				fileState.Status = "done"
				m.FileStates[i] = fileState
				m.MDoneTasks++
				m.TaskInfo[req.TaskId] = "done"
				res.Ok = true
				break
			}
		}
	} else if req.TaskType == "Reduce" {
		// Only this for now, the rest seems to be working fine by itself.
		m.TaskInfo[req.TaskId] = "done"
	}
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
	if m.NReduce == m.RAssigned {
		ret = true
		log.Println("Done, job finished!")
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

	mfStates := []MapFileState{}
	for _, file := range files {
		mfState := MapFileState{
			FileName: file,
			Status:   "awaiting",
		}
		mfStates = append(mfStates, mfState)
	}

	m.FileStates = mfStates
	m.NReduce = nReduce
	m.TaskInfo = make(map[int]string)
	m.server()
	return &m
}
