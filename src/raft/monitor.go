package raft

import (
	"log"
	"math/rand"
	"time"
)

// launchTriggerMonitor: launch the monitor which watches and triggers the leader election
func (rf *Raft) launchTriggerMonitor() {
	rand.Seed(time.Now().UTC().UnixNano())
	for {
		electionTimeout := time.Millisecond * time.Duration(rand.Int()%400+400)
		currentTime := time.Now()
		rf.mu.Lock()
		me := rf.me
		lastSeenDelta := currentTime.Sub(rf.lastUpdated)
		rf.mu.Unlock()
		// log.Println("Current Election Timeout = ", electionTimeout)
		if lastSeenDelta > electionTimeout {
			log.Printf("Node [%d] Delta: %v. Election Timeout: %v\n", me, lastSeenDelta, electionTimeout)
			rf.startElection()
		}
		// time.Sleep(5 * time.Second)
	}
}
