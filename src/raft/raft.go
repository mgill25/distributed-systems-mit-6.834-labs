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
	state     ServerState
	lastSeen  time.Duration
	timer     *time.Timer
	heartBeat *time.Timer
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
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
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

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	pid := os.Getpid()
	rand.Seed(time.Now().UTC().UnixNano() * int64(rf.me) * int64(pid))
	electionTimeout := time.Millisecond * time.Duration(rand.Intn(500)+500)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.state = Follower
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

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	state := rf.state
	me := rf.me
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if state != Leader {
		return
	}
	// log.Printf("leader [%d] sending heartbeats. term: %d", me, currentTerm)
	for peer := range rf.peers {
		if peer == me {
			continue
		}
		go func(peer int) {
			args := &AppendEntryArgs{
				Term:         currentTerm,
				LeaderId:     me,
				PrevLogIndex: -1,
				PrevLogTerm:  -1,
				Entries:      nil,
				LeaderCommit: -1,
			}
			reply := &AppendEntryReply{}
			ok := rf.sendAppendEntry(peer, args, reply)
			if ok {
				// log.Printf("❤️  Heartbeat reply from peer [%d] : %v\n", peer, reply)
				// if reply.Success {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					log.Printf("node [%d] reverting back to Follower (heartbeats)", me)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
				}
				rf.mu.Unlock()
				// }
			}
		}(peer)
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.state = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElection()
	return rf
}
