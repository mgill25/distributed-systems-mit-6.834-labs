package raft

import "log"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply = &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpdated(args.CandidateId) {
		log.Println("Granting the vote!")
		reply = &RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
		return
	}
}

