package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

// TODO: Handle concurrent data structure access

type MapFileState struct {
	FileName string
	Status   string // possible values: awaiting, assigned, done
}

// Global Data Structure
type Master struct {
	FileStates []MapFileState
	NReduce    int
	MDoneTasks int
	MAssigned  int
	RAssigned  int
	mux        sync.Mutex
}

// Worker RPCs the Master on this endpoint to request for a new task
/**

The master should notice if a worker hasn't completed its task in a reasonable
amount of time (for this lab, use ten seconds), and give the same task to a
different worker.
	- TODO: How can we achieve this?
**/

func (m *Master) GetTask(req *TaskRequest, res *TaskResponse) error {
	m.mux.Lock()
	if m.MDoneTasks == len(m.FileStates) && m.RAssigned < m.NReduce {
		res.TaskType = "ReduceTask"
		res.MDoneTasks = m.MDoneTasks
		// keep track of all reduce tasks assigned
		res.TaskId = m.RAssigned
		m.RAssigned++
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
		m.MAssigned++
	} else {
		// Map and Reduce have finished. Close the worker gracefully
		res.TaskType = "CloseWorker"
	}
	m.mux.Unlock()
	return nil
}

// Mark this particular file as done
func (m *Master) SignalMapDone(req *SignalMapReq, res *SignalMapRes) error {
	m.mux.Lock()
	for i, fileState := range m.FileStates {
		if fileState.Status == "assigned" && m.MDoneTasks < len(m.FileStates) {
			fileState.Status = "done"
			m.FileStates[i] = fileState
			m.MDoneTasks++
			res.Ok = true
			break
		}
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

	m.server()
	return &m
}
