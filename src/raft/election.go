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

3. Since we are no longer under the eye of the leader, it is perfectly fine for us to become a Candidate and Trigger our own leader election.

4. Since we are eligible for sending our a RV RPC call, we do it for all our peers.
*/

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Node [%d] %s", rf.me, "Inside startElection()\n")
	rf.electionUnderWay = true
	rf.state = "Candidate"
	gotResponses := 1 // candidate always votes for itself

	// Lets apply the rules here
	// 1. Increment the current Term
	rf.currentTerm += 1

	// 2. Vote for Self (represented via votedFor)
	rf.votedFor = rf.me

	// 3. Reset Election Timer. TODO: Verify this. I am not sure about this.
	rf.lastUpdated = time.Now()

	responseChan := make(chan int)
	// 4. Send RequestVote RPCs to all other servers
	log.Printf("Node [%d] %s", rf.me, "Sending out RPC calls\n")
	for i, _ := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  -1,
				LastLogIndex: -1,
			}
			reply := RequestVoteReply{}

			log.Printf("Node [%d] %s %d\n", rf.me, "Spawning goroutine for peer", i)
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
				log.Printf("Node [%d] %s %v\n", rf.me, "Got response, ", ok)
			}(i)
		}
	}
	// perhaps a good idea to block and await result of goroutines
	// from RPC calls?
	// TODO: But how long can we wait according to the protocol? Surely
	// we must time out eventually in case of network partitions
	gotResponses = 0
	for i := 0; i < len(rf.peers)-1; i++ {
		gotResponses += <-responseChan
	}

	// 5. If votes received from the majority, become leader. How?
	votesRequired := 2 // out of 3. hardcoded for now. TODO: Make it generic
	if gotResponses >= votesRequired {
		log.Printf("Node [%d] %s", rf.me, "Got majority votes!\n")
		rf.state = "Leader"
		go rf.SendHeartBeats(rf.currentTerm)
	}
	// Multiple nodes are trying to acquire the lock at the same time
	// If the lock is already in use, the calling goroutine blocks until the mutex is available.
}
