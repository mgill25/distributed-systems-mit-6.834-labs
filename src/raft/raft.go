package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type ServerState string

const (
	Candidate ServerState = "Candidate"
	Leader    ServerState = "Leader"
	Follower  ServerState = "Follower"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persisted
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile on all
	commitIndex int
	lastApplied int

	// Volatile on leader
	nextIndex  []int
	matchIndex []int

	// Internal
	state   ServerState
	timer   *time.Timer
	applyCh chan ApplyMsg
	cond    *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// XXX: Call site must have the Lock
func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	cIndex := args.LastLogIndex
	cTerm := args.LastLogTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpdated(cIndex, cTerm) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		pid := os.Getpid()
		rand.Seed(time.Now().UTC().UnixNano() * int64(rf.me) * int64(pid))
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
		if !rf.timer.Stop() {
			<-rf.timer.C
		}
		rf.timer.Reset(electionTimeout)
	} else {
		reply.VoteGranted = false
	}
}

// Election Restriction for Safety
func (rf *Raft) isLogUpdated(cIndex, cTerm int) bool {
	mIndex := rf.getLastLogIndex()
	mTerm := rf.log[mIndex].Term
	return cTerm > mTerm || (cTerm == mTerm && cIndex >= mIndex)
}

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
	defer rf.mu.Unlock()
	pid := os.Getpid()
	rand.Seed(time.Now().UTC().UnixNano() * int64(rf.me) * int64(pid))
	electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
	// Debug(rf, "AE args = %+v\n", args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Error(rf, "Rejecting AE: Incoming Term %d < my current Term %d", args.Term, rf.currentTerm)
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term)
		if !rf.timer.Stop() {
			<-rf.timer.C
		}
		rf.timer.Reset(electionTimeout)

		// Debug(rf, "args.PrevLogIndex = %d, len(log) = %d", args.PrevLogIndex, len(rf.log))
		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false
			return
		}
		if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			Error(rf, "Rejecting AE. Log Matching Property Fail")
			reply.Success = false
			return
		}

		var overWritten bool
		if args.Entries != nil {
			conflictIndex := -1
			// Loop through the entries and check for Index at each location in log
			for i := 0; i < len(args.Entries); i++ {
				currentEntry := args.Entries[i]
				currentEntryIndex := currentEntry.Index
				if len(rf.log) > currentEntryIndex {
					if rf.log[currentEntryIndex].Term != currentEntry.Term {
						conflictIndex = currentEntryIndex
						break
					}
				}
			}
			if conflictIndex != -1 {
				// Delete the conflict index and everything after it.
				rf.log = rf.log[:conflictIndex]
				Warn(rf, "Conflicting entries deleted from %d", conflictIndex)
				overWritten = true
			} else {
				// As per spec: Append any new Entries not already in the Log!
				// Detect if there are same entries in the log as args.Entries
				// and if yes, ignore them
				toAppend := []LogEntry{}
				for i := 0; i < len(args.Entries); i++ {
					entry := args.Entries[i]
					logIdx := entry.Index
					logTrm := entry.Term
					if len(rf.log) <= logIdx || rf.log[logIdx].Term != logTrm {
						toAppend = append(toAppend, entry)
					} else {
						Warn(rf, "Duplicate detected and ignored: %+v", entry)
					}
				}
				rf.log = append(rf.log, toAppend...)
			}

		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
			Warn(rf, "commitIndex updated = %d. min(%d, %d)", rf.commitIndex, args.LeaderCommit, rf.getLastLogIndex())
		}

		rf.applyLogEntries(args.Entries)
		reply.Success = !overWritten
	}
	// log.Printf("node[%d] Resetting timeout to %v at %v\n", rf.me, electionTimeout, time.Now())
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newEntryIndex := rf.getLastLogIndex() + 1
	entry := LogEntry{
		Command: command,
		Index:   newEntryIndex,
		Term:    term,
	}
	rf.log = append(rf.log, entry)
	Info(rf, "New entry appended: %+v", entry)
	return newEntryIndex, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ~~~~~~~~~~ election ~~~~~~~~~~~~
func (rf *Raft) runElection() {
	me := rf.me
	pid := os.Getpid()
	rand.Seed(time.Now().UTC().UnixNano() * int64(me) * int64(pid))
	electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
	rf.mu.Lock()
	rf.timer = time.NewTimer(electionTimeout)
	rf.mu.Unlock()
	var votes uint64
	voteChan := make(chan uint64)
	votesRequired := len(rf.peers)/2 + 1
	for {
		if rf.killed() {
			Error(rf, "Killed")
			break
		}
		select {
		case vote := <-voteChan:
			atomic.AddUint64(&votes, vote)
			if votes >= uint64(votesRequired) {
				rf.mu.Lock()
				rf.state = Leader
				votes = 0
				rf.initIndexMaps()
				Info(rf, "Won election, nextIndex = %v", rf.nextIndex)
				rf.mu.Unlock()
				rf.launchHeartbeats()
			}
		case <-rf.timer.C:
			rf.mu.Lock()
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = me
			rand.Seed(time.Now().UTC().UnixNano() * int64(me) * int64(pid))
			electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
			rf.timer.Reset(electionTimeout)
			currentTerm := rf.currentTerm
			Warn(rf, "Election has been started")
			rf.mu.Unlock()
			for peer := range rf.peers {
				if peer == me {
					continue
				}
				go func(peer int, voteChan chan uint64) {
					args := &RequestVoteArgs{
						Term:         currentTerm,
						CandidateId:  me,
						LastLogIndex: rf.getLastLogIndex(),
						LastLogTerm:  rf.log[rf.getLastLogIndex()].Term,
					}
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(peer, args, reply)
					if ok {
						if reply.VoteGranted {
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								rf.convertToFollower(reply.Term)
							}
							rf.mu.Unlock()
							voteChan <- 1
						} else {
							voteChan <- 0
						}
					}
				}(peer, voteChan)
			}
		}
	}
}

/**
Used for dual purpose:
	a. heartbeats
	b. log replication
*/
func (rf *Raft) sendAE() {
	rf.mu.Lock()
	state := rf.state
	me := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	if state != Leader {
		return
	}

	for peer := range rf.peers {
		// we are about to install a real entry
		if peer == me {
			continue
		}
		var prevLogIndex int
		var prevLogTerm int
		var entries []LogEntry
		rf.mu.Lock()
		lastLogIndex := rf.getLastLogIndex()
		// Debug(rf, "lastLogIndex = %d, rf.nextIndex = %v, peer = %d", lastLogIndex, rf.nextIndex, peer)
		prevLogIndex = rf.nextIndex[peer] - 1 // XXX: Why not via len(log) ?
		prevLogTerm = rf.log[prevLogIndex].Term
		if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peer] {
			entries = rf.log[rf.nextIndex[peer]:]
		} else {
			entries = nil
		}
		go rf.sendAEToPeer(peer, me, currentTerm, prevLogIndex, prevLogTerm, entries, leaderCommit)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAEToPeer(peer, me, currentTerm, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) {
	args := &AppendEntryArgs{
		Term:         currentTerm,
		LeaderId:     me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntryReply{}

	ok := rf.sendAppendEntry(peer, args, reply)
	rf.mu.Lock()

	if ok && rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
	}

	if entries != nil {
		if ok && reply.Success {
			rf.nextIndex[peer] = rf.nextIndex[peer] + len(entries)
			rf.matchIndex[peer] = args.PrevLogIndex + len(entries)
			// rf.matchIndex[peer] = entries[len(entries)-1].Index
		} else if ok && !reply.Success && rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer] -= 1
		}
	}
	// update commitIndex if we have replicated to the majority
	rf.updateCommitIndex()
	if entries != nil {
		rf.applyLogEntries(entries)
	}
	rf.mu.Unlock()
}

// Apply Log Entries: This means sending msgs over the applyCh
// Caution: Must only send the "committed" entries!
func (rf *Raft) applyLogEntries(entries []LogEntry) {
	for {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			msg := ApplyMsg{
				Command:      entry.Command,
				CommandValid: true,
				CommandIndex: entry.Index,
			}
			Info(rf, "Applied %+v", msg)
			rf.applyCh <- msg
		} else {
			break
		}
	}
}

// XXX: Call site must have the Lock
// Last rule for the Leader as per Figure 2
func (rf *Raft) updateCommitIndex() {
	// majority of matchIndex[i] must be >= N
	minMatch := rf.matchIndex[0]
	for i := 1; i < len(rf.matchIndex); i++ {
		if rf.matchIndex[i] < minMatch {
			minMatch = rf.matchIndex[i]
		}
	}

	majorityRequired := (len(rf.matchIndex) / 2) + 1
	for j := minMatch; j < len(rf.log); j++ {
		if j <= rf.commitIndex {
			continue
		}
		if rf.log[j].Term != rf.currentTerm {
			continue
		}
		// majority of matchIndex[] should be >= j
		count := 1
		for k := 0; k < len(rf.matchIndex); k++ {
			if rf.matchIndex[k] >= j {
				count++
			}
			if count == majorityRequired {
				rf.commitIndex = j
				break
			}
		}
	}
}

// ~~~~~~ heartbeats ~~~~~
func (rf *Raft) launchHeartbeats() {
	heartBeatTimeOut := 100 * time.Millisecond
	for {
		// log.Printf("❤️ ❤️ ❤️ ❤️ ❤️ ❤️ ❤️ ")
		rf.sendAE()
		time.Sleep(heartBeatTimeOut)
	}
}

// Initialize nextIndex and matchIndex
// Happens after every Election @ Leader
func (rf *Raft) initIndexMaps() {
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	totalServers := len(rf.peers)
	for server := 0; server < totalServers; server++ {
		rf.nextIndex = append(rf.nextIndex, rf.getLastLogIndex()+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
}

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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil}) // To make "real" entries start from 1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.state = Follower
	rf.applyCh = applyCh

	rf.initIndexMaps()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElection()
	return rf
}
