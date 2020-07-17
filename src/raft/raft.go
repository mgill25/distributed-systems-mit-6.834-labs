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
	"log"
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
	// log.Printf("node [%d] GetState term = %d state = %s", rf.me, term, rf.state)
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
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpdated() {
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
		// log.Printf("node [%d] voting for = %d. timer reset\n", rf.me, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) isLogUpdated() bool {
	// TODO: Implement this
	return true
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

// If Follower does not find an entry with (PrevLogIndex, PrevLogTerm)
// then it refuses the new entries
func (rf *Raft) checkEntries(PrevLogIndex int, PrevLogTerm int) bool {
	// log.Printf("node [%d] prevLogIndex = %d prevLogTerm = %d\n", rf.me, PrevLogIndex, PrevLogTerm)
	if len(rf.log) >= (PrevLogIndex - 1) {
		match := rf.log[PrevLogIndex].Term == PrevLogTerm
		return match
	}
	return false
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	pid := os.Getpid()
	rand.Seed(time.Now().UTC().UnixNano() * int64(rf.me) * int64(pid))
	electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
	// log.Printf("node [%d] AE RPC, args = %+v", rf.me, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		if len(args.Entries) > 0 {
			if !rf.checkEntries(args.PrevLogIndex, args.PrevLogTerm) {
				log.Printf("node [%d] check failed", rf.me)
				reply.Success = false
				return
			}

			/* // If an existing entry conflicts with a new one (same index different terms), */
			// delete the existing entry and all that follows it.
			// this can happen if the follower is ahead of the leader. So we must do a check for that.
			newStartIndex := args.Entries[0].Index
			if len(rf.log) > newStartIndex {
				var myEntry LogEntry
				conflictAt := -1
				for _, entry := range args.Entries {
					myEntry = rf.log[newStartIndex]
					if myEntry.Index != entry.Index || myEntry.Term != entry.Term {
						conflictAt = myEntry.Index
						break
					}
				}
				if conflictAt != -1 {
					rf.log = rf.log[:conflictAt]
					log.Printf("node [%d] Found conflict at %d, subsequent Entries truncated!", rf.me, conflictAt)
				}
			}
			// Append new entries not already in the log
			// TODO: Would we need to update the .Index of the Entries? Probably yes
			// Which is why it's probably bad design to have an explicit Index attribute
			// FIXME: This is naive append. as per spec, we need to only append entries
			// not already in the log. Which means we have to check for entries in log
			rf.log = append(rf.log, args.Entries...)
			// log.Printf("node [%d] entries appended to log!!", rf.me)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      args.Entries[0].Command,
				CommandIndex: len(rf.log) - 1,
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				log.Printf("node [%d] commitIndex updated to %d", rf.me, rf.commitIndex)
			}
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		// log.Printf("node[%d] follower log = %+v\n", rf.me, rf.log)
		if !rf.timer.Stop() {
			<-rf.timer.C
		}
		rf.timer.Reset(electionTimeout)
		reply.Success = true
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
// - Even if the Raft instance has been killed, this function should return gracefully.
// - The first return value is the index that the command will appear at
// if it's ever committed.
// - The second return value is the current term.
// - The third return value is true if this server believes it is the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// FIXME: index
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	// Start the Agreement and return Immediately
	go rf.launchAgreement(command)

	index = rf.nextIndex[rf.me] // FIXME

	return index, term, isLeader
}

func (rf *Raft) updateCommitIndex() {
	currentTerm := rf.currentTerm
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term != currentTerm {
			continue
		}
		matchCount := 0
		majorityRequired := 2
		hasMajority := false
		for j := 0; j < len(rf.matchIndex); j++ {
			if rf.matchIndex[j] >= i {
				matchCount += 1
			}
			if matchCount >= majorityRequired {
				hasMajority = true
			}
		}
		if hasMajority {
			rf.commitIndex = i
			break
		}
	}
}

// launchAgreement launces a goroutine which will take care of
// agreement of a new log entry into the system.
func (rf *Raft) launchAgreement(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logEntry := LogEntry{
		Command: command,
		Index:   len(rf.log), // Index will not be used internally only for RPC comm
		Term:    rf.currentTerm,
	}
	// The leader appends the entry to its log
	rf.log = append(rf.log, logEntry)
	log.Printf("node [%d] appended entry to self log! %v\n", rf.me, rf.log)
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      logEntry.Command,
		CommandIndex: len(rf.log),
	}
	// And then sends AppendEntry requests in parallel to all the peers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		log.Printf("peer [%d] launching agreement for entry = %+v\n", peer, logEntry)
		go func(peer int, entry LogEntry) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			n := len(rf.log)
			if !(len(rf.log) >= rf.nextIndex[peer]) {
				log.Printf("node [%d] returning early. nextIndex = %v", rf.me, rf.nextIndex[peer])
				return
			}
			var prevLogIndex int
			var prevLogTerm int
			if n >= 2 {
				// We need more than 1 *real* entries in the log
				// otherwise prevLogIndex will be meaningless
				prevLogIndex = n - 2
				prevLogTerm = rf.log[prevLogIndex].Term
			} else {
				prevLogIndex = -1
				prevLogTerm = -1
			}
			entry.Index = rf.nextIndex[peer]
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{entry},
				LeaderCommit: rf.commitIndex,
			}

			reply := &AppendEntryReply{}
			// log.Printf("leader [%d] sending AE call with args = %+v", rf.me, args)
			ok := rf.sendAppendEntry(peer, args, reply)
			if ok {
				log.Printf("node[%d] AE reply = %+v\n", rf.me, reply)
				if reply.Success {
					if rf.currentTerm < reply.Term {
						rf.convertToFollower(reply.Term)
					}
					rf.nextIndex[peer] += 1
					rf.matchIndex[peer] += 1
					rf.updateCommitIndex()
				} else {
					rf.nextIndex[peer] -= 1
				}
			}
		}(peer, logEntry)
	}
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
	votesRequired := 2 // FIXME hardcoded
	for {
		if rf.killed() {
			log.Printf("node [%d] was killed", me)
			break
		}
		select {
		case vote := <-voteChan:
			atomic.AddUint64(&votes, vote)
			if votes >= uint64(votesRequired) {
				rf.mu.Lock()
				rf.state = Leader
				term := rf.currentTerm
				rf.mu.Unlock()
				votes = 0
				log.Printf("node [%d] is now leader. Term %d\n", me, term)
				rf.launchHeartbeats()
			}
		case <-rf.timer.C:
			rf.mu.Lock()
			// log.Printf("node[%d] timer elapsed at %v\n", rf.me, time.Now())
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = me
			rand.Seed(time.Now().UTC().UnixNano() * int64(me) * int64(pid))
			electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
			// log.Printf("node[%d] Resetting timeout to %v at %v\n", rf.me, electionTimeout, time.Now())
			rf.timer.Reset(electionTimeout)
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			// log.Printf("node [%d] Started Election. Term = [%d]", me, currentTerm)
			for peer := range rf.peers {
				if peer == me {
					continue
				}
				go func(peer int, voteChan chan uint64) {
					args := &RequestVoteArgs{
						Term:         currentTerm,
						CandidateId:  me,
						LastLogIndex: -1,
						LastLogTerm:  -1,
					}
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(peer, args, reply)
					// log.Printf("node[%d] sendRequestVote to peer=%d, reply=%v", me, peer, reply)
					if ok {
						if reply.VoteGranted {
							// log.Printf("node [%d] got vote by peer [%d]!", me, peer)
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								log.Printf("node [%d] reverting back to Follower", me)
								rf.convertToFollower(reply.Term)
							}
							rf.mu.Unlock()
							voteChan <- 1
						} else {
							// log.Printf("node [%d] RV reply = %v\n", me, reply)
							voteChan <- 0
						}
					}
				}(peer, voteChan)
			}
		}
	}
}

// Caution: requires the caller function to hold `rf.mu` lock
// TODO: Think of a better design
func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.state = Follower
	log.Printf("node[%d] converted to follower", rf.me)
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	state := rf.state
	me := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	n := len(rf.log)
	rf.mu.Unlock()
	if state != Leader {
		return
	}
	// log.Printf("leader [%d] sending heartbeats. term: %d", me, currentTerm)
	for peer := range rf.peers {
		if peer == me {
			continue
		}
		go func(peer int, leaderCommit int) {
			var prevLogIndex int
			var prevLogTerm int
			if n >= 2 {
				// We need more than 1 *real* entries in the log
				// otherwise prevLogIndex will be meaningless
				prevLogIndex = n - 2
				prevLogTerm = rf.log[prevLogIndex].Term
			} else {
				prevLogIndex = -1
				prevLogTerm = -1
			}
			args := &AppendEntryArgs{
				Term:         currentTerm,
				LeaderId:     me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntryReply{}
			ok := rf.sendAppendEntry(peer, args, reply)
			if ok {
				// log.Printf("❤️  Heartbeat reply from peer [%d] : %v\n", peer, reply)
				// if reply.Success {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					log.Printf("node [%d] reverting back to Follower (heartbeats)", me)
					rf.convertToFollower(reply.Term)
				}
				rf.nextIndex[peer] += 1
				rf.matchIndex[peer] += 1
				rf.updateCommitIndex()
				rf.mu.Unlock()
				// }
			}
		}(peer, leaderCommit)
	}
}

// ~~~~~~ heartbeats ~~~~~
func (rf *Raft) launchHeartbeats() {
	heartBeatTimeOut := 100 * time.Millisecond
	for {
		// log.Printf("❤️ ❤️ ❤️ ❤️ ❤️ ❤️ ❤️ ")
		rf.sendHeartbeats()
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
	// Dummy entry at the 0th index to enforce 1-based indexing
	// in the implementation as per the Spec in the Paper
	dummyEntry := LogEntry{
		Index:   0,
		Term:    -1,
		Command: nil,
	}
	log.Printf("node[%d] adding a dummy entry to the log\n", me)
	rf.log = append(rf.log, dummyEntry)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{1, 1, 1} // TODO: better initialization
	rf.matchIndex = []int{0, 0, 0}
	rf.state = Follower
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElection()
	return rf
}
