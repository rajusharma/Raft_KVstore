Raft_KVstore
============


### Files
- raft_conf.json - This contains the election timeout and heartbeat timeout details
- peers.json - This contains the Id's and Addresses of server nodes
- raft_kvstore.go - The KV-store Raft package
- raft_kvstore_test.go - The test file for testing

### Leader election
In this first we make servers using cluster library and then using raft package choose the Leader among the nodes.
Election are held when the election timouts

There are 3 states of server nodes
- Leader
- Follower
- Candidate

Election are held in terms which have Term id.
Initially all nodes are in Follwer state and when the election timeouts the nodes becomes Candidate and increases it's term by 1 and requests vote using RequestVote RPC's from all other Followers.
If the Candidate receives majority of votes then he wins and becomes Leader.
After selection of Leader all other nodes go to Follower state.
Leader sends AppendEntries RPC to all Followers to tell that he is alive and serving as leader.
When ever any Follower timeouts and dont receives AppendEntries he goes to Candidate state increments Term id by 1 and requests votes from other nodes

Raft ensures that there is only one leader at a time.

###Log Replication
Every server has a Log in which queries or commands are stores.
At each index of Log there is LogEntry which contains index, term, and command to execute on state machine.
After leader selection leader takes queries from client and replictaes them on all followers.
When the query is replicated on majority of servers the leader commits that query and executes the command on state machine.
In our case statemachine is a KVstore implemented using Map.

### How to run
raft_kvstore.go has function New_raft(id,majority,peers file,raft conf file) which takes input the id number of server, majority value, peers file name for initializing the cluster, and raft conf file name

- type Raft interface 
    - Id() int -returns the Id of node
    - Term()     int  -returns the term id
    - isLeader() bool
    - Quit() 
    - Start()
    - Reset() //for reseting the server to follower


Now after making raft nodes we can close and again start the server using Quit() and Start() functions.
Reset the node to follower by Reset() function 
By giving delay between start and close we can see who is leader after each close and start using isLeader() fucntion.

raft_test.go is a sample test file which is documented.

The code is documented for more details.





