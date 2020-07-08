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

## FAQ

Q1. How can a server know who the current leader is at any given point in time?

Alternatively, "At any given time each server is in one of three states: leader, follower, or candidate.", are we storing this information in the state anywhere (or can these info be derived by any given node about itself, at maybe also about the leader it is following)?

- The followers **must** know who the leader is, because as per the protocol, if a follower is directly contacted by the client, it redirects the client to the leader.


- It seems the "conversion to follower" implies reseting votedFor to null/-1, *before* handling the RPC.

- Seems like `votedFor` can partially talk about this story. If a node is in candidate state, it will always vote for itself and then solicit votes from its peers. So at least for some time, votedFor will be that node's candidateId itself.

- When a follower receives a requestVote from a candidate, and assuming we follow the server rules and are able to grant a vote, we will set `votedFor` of the follower to that of the candidate (which will ideally become the new leader)

- During "de-throning" of an old leader, (according to the SO answer and the Princeton presentation), we are supposed to reset the votedFor to null (or -1, w/e). This now makes sense to me. This happens because:

        a) An old leader never partiticated in the latest leader elections, so it will never have set the latest votedFor value

        b) Because it was an old leader, it's votedFor would still be itself when it rejoins a cluster after a network partition. That needs to be corrected. It needs to revert back to follower state after missing 1 or more newer elections.

        c) Therefore, it is a follower who never voted for a leader. Resetting its votedFor = -1 makes the most logical sense.

ref: https://www.cs.princeton.edu/courses/archive/fall18/cos418/docs/p7-raft.pdf
ref: https://stackoverflow.com/a/50548740

- the SO answer implies that when votedFor = null, it simply means that the node is a follower and it will always vote for the requesting server. Unsure about this also.

- One way to detect who the leader is could also be the fact that the leader is always going to vote for itself. So if a candidateId == votedFor, we _might_ be a leader. We could also be a candidate undergoing a leader election, but I think in normal operations, it is fair to assume that votedFor == candidateId will only be true for a leader. Yay :)

## Okay, so we know that votedFor is used to store the state of who we possibly voted for in the last election we participated in (including ourselves)

## How can we find out who the leader is?

        - Technically the leader is the one whose votedFor == candidateId
        - But that is not enough to *tell* others that "I just became the leader, y'all listen to me now!"
        - Perhaps for this, we need to start sending our own Heartbeat requests as soon as it is computed that we are now the leader. And when the others become followers, that automatically implies that they will stop sending further RV requests and will start receiving AE entries from the leader.

        - Is there merit to this idea?

## Election Timer:
        - Raft uses randomized election timer: so there is a `rand()` component to it.
        - Timer gets "reset" when an election starts
        - Election starts when election timeout elapses
        - Election Timeout elapses when a follower receives no communication from leader for a given period of time
        - Election *Timeout* is the one that is picked randomly. According to the paper, any value b/w (150, 300) ms

Paper: 
        - "Each candidate restarts its randomized election timeout at the start of an election"
        - "It waits for that timeout to elapse before starting a new election"

[Secret Lives of Data](http://thesecretlivesofdata.com/raft/#election)
2 Timeout Settings that control elections in Raft:
        1. Election Timeout: 
                - "The election timeout is the amount of time a follower waits until becoming a candidate."
                - Randomized between 150ms and 300ms
                - Candidate Node resets its election timeout after receiving vote
                - TODO: What happens when a candidate fails to receive necessary votes? Do we still reset the timeout?
        
        2. Heartbeat Timeout:
                - Leader begins sending out AppendEntry messages to Followers
                - These are sent in intervals specified by the "Heartbeat timeout"

        Heartbeats from the leader are the ones that are continuously reseting the election timeout at the followers
        (This must reason that heartbeats must go out far more frequently compared to the election timeout setting)

        - "Does not hear from the leader in time Ta": Become Candidate

## During Election
A node goes in the candidate state when it hasn't heard from the leader in `election_timeout` time. When this happens, the node starts a RequestVote RPC and waits to hear from the peers. It has already voted for itself.

Now, the node continues to be in the candidate state until one of the following happens:

1. It wins the election
2. Another server establishes itself as the leader
3. A period of time goes by with no winner

### On winning election

Once a candidate becomes a leader, it starts sending out heartbeat messages to the peers.
What happens with the election timeout in that case? Presumably nothing? A leader doesn't need to listen to heartbeats, it is the one that is sending them out.

Okay, so during the development of the leader, we need to start thinking about the heartbeat mechanism and how it works. Perhaps in its own goroutine, where the monitor communicates with it via channels? Or something

### On Losing to another Leader

We need to revert back to the follower state since we lost the election. How do we handle the election timeouts now? Also, btw, what should be the value of `votedFor` in this case? If we voted for ourselves and _still lost_, we can't really claim that votedFor == candidateId will always result in the leader. So perhaps that assertion of ours was wrong. Perhaps the only sign of a winning leader is that the leader is sending the AE requests (and maybe we also store an internal state)

### No winner/Split Decision

In this case, each candidate will "time out", increment its term and start a new election

"Each candidate restarts its randomized election timeout at the start of the election, and it waits for that timeout to elapse before starting a new election"

        - So, randomized timeouts because we didn't hear from the leader are OK
        - _new_ randomized timeout because the previous election failed are TODO
        - Can be done if we store state of previous election and the time at which it failed, and then taking a delta with a new randomized timeout value. Possible...

        - Oh okay, it says that when an election is started, we "reset the election timer": WHAT DOES THIS MEAN?
        - Perhaps we simply update the `lastUpdated` value. Lets give this a try

### Bugs

- I have multiple leaders winning elections and sending out heartbeats
- RequestVote RPCs block and fail to return   [FIXED]
- RequestVote RPCs are serial, not concurrent [FIXED]
- LiveLocks and no leader is being elected

2020/07/08 12:16:20 Node [0] Got reply from  1 {0 true}
2020/07/08 12:16:20 Node [2] Got reply from  1 {0 true}

A node is sending true in reply to multiple RequestVote calls. This should not happen, and as soon as a node has voted for someone, it should reject all subsequent request votes.

2020/07/08 12:46:35 Node [0] Peer[1] says {0 true}
2020/07/08 12:46:35 Node [2] Peer[1] says {0 true}

* Why is Peer[1] replying `true` multiple times? Why is it not following the Figure2 Rules of Raft?

- labgob warning: Decoding into a non-default variable/field Term may not work

* I might have to re-tweak electionTimeout (or HeartBeatTimeout) again. It seems like there are too many candidates (which means too many nodes getting timed out within a short amount of time). This is not an unlikely scenaio and should ba handled on its own, but worth looking into why it happens. Perhaps the test is written to simulate such a case?


## Election Timeout fine grained rules

- monitor, watching the election timeout
- rpc handlers

* election timeout is reset in the rpc handlers
* election is triggered based on the timeout in the monitor

**Follower Scenario**
From Figure 2: *"If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate"*

If election timeout elapses without 
        - Receiving AppendEntries RPC from current leader OR
        - Without Granting vote to Candidate
Then
        - Convert to Candidate
-------------------------------------------------------------

During the Election Timeout elapse check, we need to do 2 additional checks:
        - The timeout happened without receiving any AE RPC from the current leader
        - The timeout happened _without_ granting vote to a candidate

Possible implication: When we receive an AE RPC from someone who is not our current leader, we do not reset the timeout and revert to candidate state eventually.

Possible implication: When we do not grant vote to a candidate, we don't reset the timeout.

In both of the above implications, we can simply store internal state and use those internal state checks in our monitor for conversion to Candidate.
The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. 

Such a range only makes sense if the leader sends heartbeats 
        considerably more often than once per 150 milliseconds. 

Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 
        150 to 300 milliseconds, but not too large,
because then you may fail to elect a leader within five seconds. 

----------------------


Election Timeout, despite having a broad range, always gets stuck around the same value for different instances of Raft. This happens despite having a unique unix UTC time based RNG seed that gets initialized at the start of the process. 

This is very strange.
