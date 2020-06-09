package raft

// LogEntry represents a single log entry in the entire log
// What does a Log Entry need? Everything that is required to
// construct the replicated state machine! What is that?
type LogEntry struct {
	Command interface{}
	Term 	int
}

