package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Adding the Worker args and reply struct for RPC
type TaskRequest struct {
	// what does the worker need to tell Master?
	// that I am available? that should be obvious because it is calling!
	// maybe nothing for now?
}

type TaskResponse struct {
	// What does the Master need to reply back with?
	FileName   string
	TaskType   string 		// MapTask/ReduceTask
	TaskId     int
	NReduce    int
	MDoneTasks int
}

type SignalMapReq struct {
	FileName string
}

type SignalMapRes struct {
	Ok bool
}