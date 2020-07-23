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
	if args.Term < rf.currentTerm {
		Error(rf, "Rejecting AE: Incoming Term %d < my current Term %d", args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term >= rf.currentTerm {
		var overWritten bool
		// Debug(rf, "prev(index, term) = (%d, %d). log len = %d", args.PrevLogIndex, args.PrevLogTerm, len(rf.log))
		if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			Error(rf, "Rejecting AE. Log Matching Property Fail")
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		if args.Entries != nil {
			// Debug(rf, "entries = %v", args.Entries)
			conflictIndex := -1
			// Loop through the entries and check for Index at each location in log
			i := 0
			for {
				if i >= len(args.Entries) {
					break
				}
				currentEntry := args.Entries[i]
				currentEntryIndex := currentEntry.Index
				if len(rf.log) > currentEntryIndex {
					// Debug(rf, "log len = %d, currentEntryIndex = %d", len(rf.log), currentEntryIndex)
					if rf.log[currentEntryIndex].Term != currentEntry.Term {
						conflictIndex = currentEntryIndex
						break
					}
				}
				i += 1
			}

			if conflictIndex != -1 {
				// Delete the conflict index and everything after it.
				rf.log = rf.log[:conflictIndex]
				Warn(rf, "Conflicting entries deleted from %d", conflictIndex)
				overWritten = true
			} else {
				rf.log = append(rf.log, args.Entries...)
				// Debug(rf, "Follower log appended. Log = %v", rf.log)
				for _, entry := range args.Entries {
					msg := ApplyMsg{
						Command:      entry.Command,
						CommandValid: true,
						CommandIndex: entry.Index,
					}
					rf.applyCh <- msg
				}
			}

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
			}
		}

		rf.convertToFollower(args.Term)

		if !rf.timer.Stop() {
			<-rf.timer.C
		}
		rf.timer.Reset(electionTimeout)

		reply.Success = !overWritten
	}
	// log.Printf("node[%d] Resetting timeout to %v at %v\n", rf.me, electionTimeout, time.Now())
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// call AppendEntry
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
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
	Info(rf, "New entry appended. Log Len = %d, newEntryIndex = %d", len(rf.log), newEntryIndex)
	msg := ApplyMsg{
		Command:      command,
		CommandValid: true,
		CommandIndex: newEntryIndex,
	}
	rf.applyCh <- msg
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
				rf.mu.Unlock()
				votes = 0
				Info(rf, "Won election")
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
		if peer == me {
			continue
		}
		// we are about to install a real entry
		var prevLogIndex int
		var prevLogTerm int
		var entries []LogEntry
		rf.mu.Lock()
		lastLogIndex := rf.getLastLogIndex()
		// Debug(rf, "lastLogIndex = %d, rf.nextIndex = %v, peer = %d", lastLogIndex, rf.nextIndex, peer)
		if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peer] {
			prevLogIndex = rf.nextIndex[peer] - 1 // XXX: Why not via len(log) ?
			prevLogTerm = rf.log[prevLogIndex].Term
			entries = rf.log[rf.nextIndex[peer]:]
		} else {
			prevLogIndex = -1
			prevLogTerm = -1
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

	if ok && reply.Success {
		if entries != nil {
			// TODO: Is this correct?
			// Maybe. Maybe Not. Maybe Fuck you.
			rf.nextIndex[peer] += len(entries)
			Debug(rf, "Incremented nextIndex[%d] to %d", peer, rf.nextIndex[peer])
		}
	} else {
		if entries != nil {
			// TODO: This if check might be a hack. Dunno
			if rf.nextIndex[peer] > 1 {
				Warn(rf, "Decrementing nextIndex for peer %d, new nextIndex = %v", peer, rf.nextIndex)
				rf.nextIndex[peer] -= 1
			}
		}
	}
	rf.mu.Unlock()
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

	totalServers := len(rf.peers)
	for server := 0; server < totalServers; server++ {
		rf.nextIndex = append(rf.nextIndex, rf.getLastLogIndex()+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.state = Follower
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElection()
	return rf
}
