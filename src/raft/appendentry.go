package raft

import (
	"time"
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastUpdated = time.Now()
	if args.Term < rf.currentTerm {
		reply.Term = -1
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		reply.Success = true // FIXME: This is incomplete
	}
	return
}
