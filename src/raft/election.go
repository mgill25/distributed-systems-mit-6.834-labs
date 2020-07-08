package raft

import (
	"log"
	"time"
)

// Triggers off the election via RV RPC calls if we have not heard
// from another peer in a while.
// We hear from peers via heartbeats: AE requests without any log entries
/**
This method will do a few things:

1. Check the internal raft state. Specifically, we check if `lastUpdated` has changed and compare it to the current time.

2. If lastUpdated hasn't changed in some time (controlled by threshold), then we assume we have not heard from the Leader. This assumption is rooted in the fact that any heartbeat or AppendEntry RPC call that the leader sends to the follower must necessarily change the rf.lastUpdated value.

3. Since we are no longer under the eye of the leader, we become a Candidate and Trigger our own leader election.

4. Since we are eligible for sending our a RV RPC call, we do it for all our peers.
*/

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.electionUnderWay = true
	rf.state = "Candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastUpdated = time.Now()

	log.Printf("Node [%d] Term [%d] %s", rf.me, rf.currentTerm, "Inside startElection()\n")

	me := rf.me
	currentTerm := rf.currentTerm

	rf.mu.Unlock()

	responseChan := make(chan int)
	for i, _ := range rf.peers {
		if i != me {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  me,
				LastLogTerm:  -1,
				LastLogIndex: -1,
			}
			reply := RequestVoteReply{}

			// log.Printf("Node [%d] %s %d\n", me, "Spawning sendRequestVote goroutine for peer", i)
			go func(i int) {
				ok := rf.sendRequestVote(i, &args, &reply) // might be unreliable
				if ok {
					if reply.VoteGranted {
						responseChan <- 1
					} else {
						responseChan <- 0
					}
				} else {
					responseChan <- 0
				}
				log.Printf("Node [%d] Peer[%d] says %v\n", me, i, reply)
			}(i)
		}
	}
	log.Printf("Node [%d] %s\n", me, "Listening on responseChan")
	gotResponses := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		gotResponses += <-responseChan
	}
	votesRequired := 2 // out of 3. TODO: Make it generic
	if gotResponses >= votesRequired {
		log.Printf("Node [%d] %s", me, "Got majority votes!\n")
		rf.mu.Lock()
		rf.state = "Leader"
		rf.mu.Unlock()
		go rf.SendHeartBeats(currentTerm, me)
	}
}
