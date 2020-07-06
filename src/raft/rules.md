# Rules

## [Locking Rules](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt)

## Rules for all servers

* If RPC request or response contain term T > currentTerm:
        - set currentTerm = T
        - "convert to follower"

On conversion to candidate, start election:
    • Increment currentTerm
    • Vote for self
    • Reset election timer
    • Send RequestVote RPCs to all other servers
    • If votes received from majority of servers: become leader
    • If AppendEntries RPC received from new leader: convert to follower
    • If election timeout elapses: start new election
