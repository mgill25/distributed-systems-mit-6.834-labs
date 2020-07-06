# Labs

## 2A

### Heartbeats
To implement heartbeats, 
        - define an AppendEntries RPC struct (though you may not need all the arguments yet), 
        - have the leader send them out periodically. 
        - Write an AppendEntries RPC handler method that resets the election timeout 
                So that other servers don't step forward as leaders when one has already been elected. 


Okay, so as soon as we have a leader, we can launch a Heartbeat mechanism from the leader.

Cool, so we have heartbeats.

What we don't have is correct implementation of Server Rules. We need those rules in order to make sure the protocol is working correctly. We will figure that out :)
