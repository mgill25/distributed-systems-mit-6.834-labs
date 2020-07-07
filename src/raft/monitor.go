package raft

import (
	"time"
)

// launchTriggerMonitor: launch the monitor which watches and triggers the leader election
func (rf *Raft) launchTriggerMonitor() {
	for {
		rf.watchAndTriggerElection()
		time.Sleep(1 * time.Second) // Just for sanity right now. Might not be needed
	}
}

func (rf *Raft) watchAndTriggerElection() {
	rf.mu.Lock()
	currentTime := time.Now()
	lastSeenDelta := currentTime.Sub(rf.lastUpdated)
	electionTimeout := rf.GetElectionTimout() * time.Second
	rf.mu.Unlock()
	if lastSeenDelta > electionTimeout {
		rf.startElection()
	}
}
