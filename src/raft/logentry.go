package raft

// LogEntry represents a single log entry in the entire log
// Each log entry contains the command for state machine, and the term
// when the entry was received by the leader. first index = 1
type LogEntry struct {
	Command interface{}
	Term 	int
	Index   int  // TODO: Unsure if needed, just adding as precaution
}

