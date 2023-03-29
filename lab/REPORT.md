# Project 4: Report
## Intro (Your understanding)
In lab 3, we implemented a viewserver protocol that provides clients information on which server to contact as primary service. 
However, the viewserver protocol assume that the viewserver itself is always available, thus this is the single point of failure in the system.
In this lab, we go one step further and implement a consensus protocol such that if there are 2N + 1 servers, we can tolerate up to N failures.
The consensus protocol we'll be working with is called Paxos. Paxos is a protocol that allows a group of servers to agree on a value.
However, the simple paxos protocol is inefficient in that it only allows one value to be agreed upon at a time. Thus we will be implementing a variant of paxos called Multi-Paxos, 
which allows multiple values to be agreed upon at the same time. 
We are expected to have read [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) 
and [Paxos Made Moderately Complex](http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf) as our starting point.

## Flow of Control & Code Design
In the PMMC paper, the authors separated the Paxos protocol into 3 parts: the proposer/replica, the leader, and the acceptor. 
However, in our implementation, we will combine all three roles for each server. 

### Stable Leader Election
Unlike the PMMC implementation where leader that get preempted by another higher ballot leader try to compete for leadership again,
we have a stable leader election where preempted leader will switch to follower role and wait for the new leader to send a heartbeat to it.
This is to prevent the leader from competing for leadership again and again, which will cause the system to be unstable. The leader election is 
done using the similar implementation as P1A and P1B in the PMMC paper. Server with lower ballot number, when receiving a P1A message, will reply with a P1B message 
with the ballot number of the sender along with the list of Pvalue (proposed slot number and value from the highest ballot number it has seen). This list lets the new leader 
know what values has been proposed by other prior leaders. The new leader will then pick the highest ballot number and the corresponding value to propose to the acceptors.
In our implementation, there could be a time where multiple nodes might believe that they are the leader. The paxos protocol will ensure that only one value will be agreed upon.
Whenever a "fault" leader sends an accept request to the acceptors, the acceptors will reject the request and send a reject message to the leader. The leader will then know that it is preempted and will switch to follower role.
### Leader/Proposer
Once a stable leader is elected, it acts as both the leader and the proposer. Clients will broadcast their request to all servers. 
However, only the leader will accept the request and send a accept request to all servers. The accept request contains the slot number and the ballot number as well as the client request.
The leader will then wait for a majority of acceptors to reply with an accept or reject message. Once a majority of acceptors, the leader will add the request to its log and send a commit message to all servers.

### Acceptor/Replica/Follower
The logic for acceptor is pretty simple. It will accept the election request if the ballot number is higher than the current ballot number.
The same logic applies to the accept request. If the ballot number is higher or equal to the current ballot number, it will add the request to the accept list and send an accept message back to the leader.
Once the leader receives a majority of accept messages, it will send a commit message to all servers. Being that replica and follower are the same thing, this server will 
simply add the request to its log.

### Commit to log and execute operation
Since we are implementing a multi-paxos protocol,multiple operation are being decided at the same time. We assume that message can be lost and reordering can happen.
Thus, we need to make sure that the requests are executed in the same order as they are committed to the log.
We will use a commit index (slot out) to keep track of the next slot that needs to be executed. Once the operation is executed, the commit index will be incremented by 1 and the leader will send the reply
to the client.

### Client
The client will broadcast the request to all servers and periodically check if it has received a reply and resend the request if needed.
Paxos consensus protocol acts in a way that presents the group of server as a single entity.

## Design Decisions

### Use of locality
When talking about locality here, we are referring to the fact that all three roles are implemented in the same server. 
When the acceptor receives a election/accept request, it will check if the ballot number is higher than the current ballot number.
If it is, we can assume that the leader on the same server is no longer the leader and thus we can preempt and switch to follower mode. 

### Batch Requests
During the initial election phase, each server could be receiving multiple requests from clients. By adding these requests to a list, 
we can batch the requests and send them to acceptors once the leader is elected. This will reduce the number of messages sent during the initial phase.
the downside of this is that if the message is lost, the leader will have to resend the whole batch of requests to the acceptors, instead of just the one request that was lost.

In additional, We have a timer that go off periodically to check if the leader has received a majority of accept messages. 
If it has not, it will resend the batch of requests to the acceptors.

### Detecting leader failure
When a leader is elected, it will send a heartbeat message to all servers periodically. If a server does not receive a heartbeat message from the leader for a certain amount of time,
it will start a new election. This is to guarantee that the system is able to move on if the leader fails. 
We have implemented randomized delay for election to prevent multiple servers from starting an election at the same time, creating contention for the leader role.

### Catchup Mechanism
There are two catchup mechanism that we have implemented. The first one is to catchup for the new candidate that is trying to become the leader. 
When receiving a  P1A message with higher ballot, acceptor will reply  with a P1B message along with the list of Pvalue and also its decisions log.
This allows the candidate to compare with its own log and catch up if needed. If we compare this to RAFT implementation, this is a opposite as in RAFT, the candidate with the most up-to-date log will be elected. 

The second catchup mechanism is for the follower to catch up with the leader as commit messages could be lost. 
During heartbeat, the leader will piggyback its decisions log to the follower. If the follower's log is behind, it will catch up with the leader and reply with its commit index.

### Garbage Collection
We use a vector clock to keep track of the commit index for each server thus allow us to decide when to garbage collect the log.
The vector clock is generally kept up by the leader. When the leader receives a reply from its heartbeat message with the commit index from each server, 
it will update the vector clock. In each heartbeat message, the leader will tabulate the vector clock and send garbage collection index for server to garbage collect.

This use of vector clock assume that at some point, the all follower will be able to communicate with the stable leader. However, if we constantly have network partition,
we would need to use a matrix clock to fulfill the garbage collection requirement. This matrix clock will let each server keep track of the commit index from everyone else. 

### Use of 1 timer and multiple counters
To reduce the number of timers set, we have implemented a single timer on the server and multiple counters to keep track of the different timeouts.
we have a counter for detecting leader failure/starting election as we don't want to start election right away after missing a heartbeat, 
a counter for resending the batch of requests so that we don't resend the batch of requests too often or too late, 
and immediately retry for the heartbeat message and election message as we want to make sure the liveness of the system. 

## Missing Components
### No-op implementation
It was suggested by ellismichael slides that we can use no-op to tolerate holes in the log (i.e when a server receives a chosen value
for slot n -1 and n + 1 but not n). This hole can result in the system being unable to make progress as we need to make sure linearizability is maintained.
However, we optimize this case by checking our proposal and decided list to see if we have a value for the slot number that we are missing and use this value
for the next request.

## References
1. [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
2. [Paxos Made Moderately Complex](http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf)
3. [Paxos Made Moderately Complex in JAVA](https://www.cs.cornell.edu/home/rvr/Paxos/paxos.pdf)
4. [Ellis Michael Slides](https://ellismichael.com/dslabs/452%20Discussion%20Section%20Slides.pdf)
5. [“Paxos Made Moderately Complex” Made Moderately Simple](https://courses.cs.washington.edu/courses/cse452/20sp/slides/pmmc.pdf)

## Extra (Optional)