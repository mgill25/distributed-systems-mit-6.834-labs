package mr

import (
	"os"
	"strconv"
)

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
	FileName      string
	TaskType      string // MapTask/ReduceTask
	TaskId        int
	NReduce       int
	NumInputFiles int
	Msg           string
}

type DoneReq struct {
	FileName string
	TaskId   int
	TaskType string
}

type DoneRes struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
