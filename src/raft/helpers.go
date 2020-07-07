package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) GetElectionTimout() time.Duration {
	max := 4
	min := 1
	randTime := rand.Intn(max-min) + min
	return time.Duration(randTime)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// isLogUpdated Check if the candidate's log is at least as up-to-date
// as receiver's log, grant vote.
func (rf *Raft) isLogUpdated(candidateId int) bool {
	// TODO
	return true
}
