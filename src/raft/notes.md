
# Brainstorming on Raft

Command is appended to the replicated log!
Perhaps, the Log Entry needs Command interface{}

2 RPC functions:
	sender (SendRequestVote)
	receiver (RequestVote)

	RPCs use labrpc module

2A Implementation
	* Leader election
	* Heartbeats (AppendEntries RPC with no Log Entries)
	* Single leader should be elected (this is what the tester tests for)


- We should care about 
	- sending and receiving RequestVote RPCs
	- election rules
	- leader election related State