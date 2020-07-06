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
	"sync"
	"time"

	"../labrpc"
)

// import "time"

// import "bytes"
// import "../labgob"

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

	// Volatile on all servers
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry "applied" to the state machine

	// Volatile on leaders (reinitialized after election)
	// nextIndex: For each follower, index of the next log entry to send to that server.
	// initialized to (leader's last log index + 1)
	nextIndex []int
	// MatchIndex: For each server, index of highest log entry known to be replicated on server.
	// Initialized to 0, increases monotonically
	matchIndex []int

	// Persisted state on all servers
	log         []LogEntry // first index = 1 in the log (acc to paper)
	votedFor    int        // candidateId that received the vote in the current term (or null if None)
	currentTerm int        // latest term the server has seen. initialized to 0 on boot. increases monotonically

	// Internal
	lastUpdated      time.Time
	logIndex         int
	electionUnderWay bool
	state            string
}
