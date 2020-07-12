package raft

import (
	"log"
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
	log.Printf("Node [%d] AE RPC lock acquired", rf.me)
	defer rf.mu.Unlock()
	// log.Printf("Node [%d] lastUpdated before heartbeat: %v\n", rf.me, rf.lastUpdated)
	// log.Printf("Node [%d] lastUpdated after  heartbeat: %v\n", rf.me, rf.lastUpdated)
	if args.Term < rf.currentTerm {
		reply.Term = -1
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		reply.Success = true // FIXME: This is incomplete
		reply.Term = rf.currentTerm
		rf.lastUpdated = time.Now()
	}
	log.Printf("Node [%d] AE RPC lock released", rf.me)
	return
}
