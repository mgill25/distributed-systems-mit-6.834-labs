package raft

import (
	"time"
)

/**
 The tester requires that the leader send heartbeat RPCs no more than ten times per second.

 => Heartbeat Frequency should be <= 10 Hz.
 How can I control the timer frequency?

 for {
	 heartBeat();
	 sleep(t)
 }
 t = 1/10 of a second = 100 milliseconds

 ---Relationship b/w election timeout and heartbeat timeout---
 By example: If election timeout is say 300ms, then we only wait a max of 300ms before becoming a
 candidate. This means that the heartbeats need to come much more rapidly as compared to 300ms

 * heartbeat timeout << election timeout

**/

// This will be launched as a Goroutine from Monitor (or main?)
// Repeated heartbeats to all peers with a timeout
func (rf *Raft) SendHeartBeats(currentTerm int) {
	heartBeatTimeOut := 100 * time.Millisecond
	for {
		rf.HeartBeat(currentTerm)
		time.Sleep(heartBeatTimeOut)
	}
}

func (rf *Raft) HeartBeat(currentTerm int) {
	// log.Println(rf.me, "Sending out heartbeat...")
	args := AppendEntryArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	reply := AppendEntryReply{}
	for i := range rf.peers {
		if i != rf.me {
			ok := rf.sendAppendEntry(i, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.lastUpdated = time.Now()
			}
		}
	}
}
