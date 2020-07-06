package raft

import (
	"log"
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	votedFor := rf.votedFor
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	if args.Term < currentTerm {
		log.Println(me, "My term is greater than yours, No bueno")
		reply = &RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: false,
		}
		return
	}
	if votedFor == -1 && rf.isLogUpdated(args.CandidateId) {
		// TODO: "votedFor is null **or candidateId**
		log.Println(me, "I am Granting the vote!")
		reply = &RequestVoteReply{
			Term:        currentTerm + 1,
			VoteGranted: true,
		}
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		return
	}
	// I should never be stuck in the limbo! Lets check our enforcement of the Rules
	log.Println(me, "~~~~~~~~~~~~~~Stuck in a limbo~~~~~~~~~~~~~~")
	return
}
