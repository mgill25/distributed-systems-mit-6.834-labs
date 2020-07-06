package raft

import (
	"fmt"
	"log"
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
	fmt.Println(rf.me, "Election timeout = ", electionTimeout)
	rf.mu.Unlock()
	if lastSeenDelta > electionTimeout {
		log.Println("[", rf.me, "] Triggering Leader Election after", lastSeenDelta)
		rf.startElection()
	}
}

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
	rf.electionUnderWay = true
	rf.state = "Candidate"
	gotResponses := 1 // candidate always votes for itself

	// Lets apply the rules here
	// 1. Increment the current Term
	rf.currentTerm += 1

	// 2. Vote for Self (represented via votedFor)
	fmt.Println(rf.me, " voting for myself haha")
	rf.votedFor = rf.me

	// 3. Reset Election Timer...how? TODO: Verify this. I am not sure about this.
	rf.lastUpdated = time.Now()

	// 4. Send RequestVote RPCs to all other servers
	log.Println("Sending out RV RPCs...")
	for i, _ := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  -1,
				LastLogIndex: -1,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply) // might be unreliable
			if ok {
				log.Println(rf.me, "Got RV response: ", reply.VoteGranted)
				gotResponses += 1
			}
		}
	}
	log.Println("Collectting RV RPC responses")

	// 5. If votes received from the majority, become leader. How?
	votesRequired := 2 // out of 3. hardcoded for now. TODO: Make it generic
	if gotResponses >= votesRequired {
		log.Println("GOT MAJORITY VOTES, NODE", rf.me, "SHOULD BECOME LEADER")
		rf.state = "Leader"
		go rf.SendHeartBeats(rf.currentTerm)
	}
	log.Println(rf.me, "Unlocking")
	rf.mu.Unlock()
}
