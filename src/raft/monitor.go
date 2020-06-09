package raft

import (
	"log"
	"time"
)

// TODO: This is supposed to be a background monitor goroutine
// Unsure how this pattern will be coded up in Go. Lets see later.
func (rf *Raft) launchTriggerMonitor() {
	log.Println("Launching leader election watcher")
	for {
		rf.triggerElection()
		time.Sleep(10 * time.Second) 	// Just for sanity right now. Might not be needed
	}
}

// Triggers off the election via RV RPC calls if we have not heard
// from another peer in a while.
// We hear from peers via heartbeats: AE requests without any log entries
func (rf *Raft) triggerElection() {
	currentTime := time.Now()
	THRESHOLD := time.Second * 5 	// TODO
	lastSeenDelta := currentTime.Sub(rf.lastUpdated)
	log.Println("last seen delta = ", lastSeenDelta)
	if lastSeenDelta > THRESHOLD {
		// We are eligible for sending out an RV
		/* TODO: Instead of hearing from all peers, we are only supposed to check if we've heard from the Leader or not. Since the leader is the one sending out the heartbeats. */
		for i, _ := range rf.peers {
			if i != rf.me {
				log.Println(rf.me, "sending out an RV to peer ", i)
				// Now we send out an RPC...using labrpc library?
				args := RequestVoteArgs{
					Term: -1,
					CandidateId: -1,
					LastLogTerm: -1,
					LastLogIndex: -1,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply) // might be unreliable
				if ok {
					log.Println("Got RV response: ", reply.VoteGranted)
				}
			}
		}
	} else {
		log.Println("Easy tiger, peers are still in touch. No election for you")
	}
}
