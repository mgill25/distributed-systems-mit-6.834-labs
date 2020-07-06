package raft

import "log"

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Println(rf.me, "Inside RequestVote RPC Handler. My current Term = ", rf.currentTerm)
	log.Println(rf.me, "args=", args)
	if args.Term < rf.currentTerm {
		log.Println(rf.me, "Sorry, I am not granting the vote")
		reply = &RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpdated(args.CandidateId) {
		log.Println(rf.me, "I am Granting the vote!")
		reply = &RequestVoteReply{Term: rf.currentTerm + 1, VoteGranted: true}
		return
	}
}
