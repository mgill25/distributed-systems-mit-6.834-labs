package raft

import "math/rand"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	var choices = []bool{
		true,
		false,
		false,
	}
	isleader = choices[rand.Intn(len(choices))] // TODO: This is BADADDDDD!!!!!!!!!!! (or is it?)
	return term, isleader
}

// isLogUpdated Check if the candidate's log is at least as up-to-date
// as receiver's log, grant vote.
func (rf *Raft) isLogUpdated(candidateId int) bool {
	// TODO
	return true
}

