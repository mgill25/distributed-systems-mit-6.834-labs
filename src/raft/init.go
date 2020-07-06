package raft

import (
	"time"

	"../labrpc"
)

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = "Follower"

	// Your initialization code here (2A, 2B, 2C).
	// currentTerm: latest term server has seen (initialized to 0 on first boot, increases monotonically)
	rf.currentTerm = 0

	// votedFor: candidateId that received vote in current term (or null if none)
	rf.votedFor = -1

	// log entries; each entry contains command for state machine, and
	// term when entry was received by leader (first index is 1)
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		// nextIndex initialized to last log index + 1 for each follower
		// TODO: should we ignore it for leader itself, or let it be?
		rf.nextIndex[i] = rf.logIndex + 1
	}

	rf.lastUpdated = time.Now()

	// Launch a long running background worker which can trigger an election if needed
	go rf.launchTriggerMonitor()

	// TODO: When do we persist the state initially?
	// rf.persist()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
