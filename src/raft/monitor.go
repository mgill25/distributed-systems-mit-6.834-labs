package raft

import (
	"math/rand"
	"os"
	"time"
)

// launchMonitor: launch the monitor which watches and triggers the leader election
func (rf *Raft) launchMonitor(me int) {
	pid := os.Getpid()
	// Repeatedly watch, until it has been too long till we have heard from
	// another node. This is the election timeout setting.
	for {
		rand.Seed(time.Now().UTC().UnixNano() * int64(me) * int64(pid))
		// Pick a random timeout b/w 500ms and 700ms
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(200)+500)
		// log.Printf("Waiting %v\n", electionTimeout)
		select {
		case <-time.After(electionTimeout):
			rf.mu.Lock()
			lastUpdated := rf.lastUpdated
			rf.mu.Unlock()
			delta := time.Now().Sub(lastUpdated)
			if delta > electionTimeout {
				// log.Printf("Node [%d] delta = %v; timeout was = %v\n", me, delta, electionTimeout)
				rf.startElection()
			}
		}
	}
}
