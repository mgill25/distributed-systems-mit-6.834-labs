package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) GetElectionTimout() time.Duration {
	max := 1000
	min := 500
	randTime := min + rand.Intn(max)
	return time.Duration(randTime)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "Leader"
}

// isLogUpdated Check if the candidate's log is at least as up-to-date
// as receiver's log, grant vote.
func (rf *Raft) isLogUpdated(candidateId int) bool {
	// TODO
	return true
}
