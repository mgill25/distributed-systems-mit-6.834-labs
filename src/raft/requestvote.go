package raft

import (
	"log"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// log.Printf("Node [%d] %s\n", rf.me, "Inside RequestVote handler")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("RV Handler Node [%d] Term [%d] State [%s]\n", rf.me, rf.currentTerm, rf.state)

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 && rf.isLogUpdated(args.CandidateId) {
		// 2. If votedFor is null or CandidateId, and Candidate's log is at least
		// as up to date as receiver's log, grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// (Rules for Servers)
		// If RPC request or response contains term T > currentTerm,
		// set currentTerm = T, convert to follower
		if args.Term > rf.currentTerm {
			rf.state = "Follower"
			rf.currentTerm = args.Term
		}
	} else {
		reply.VoteGranted = false
	}
	rf.lastUpdated = time.Now()
	log.Printf("RV END Node [%d] Term [%d] State [%s] VoteGranted [%v]\n", rf.me, rf.currentTerm, rf.state, reply.VoteGranted)
	return
}
