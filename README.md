
#################################################################

PAXOS

#################################################################

1. This folder contains implementation of Multi-Paxos Protocol as described in the paper
"Paxos Made Simple" by Leslie Lamport

2. The implementation is for simulated network where clients and servers are just
separate go routines inside one process, and communicate using RPCs implemented over go channels.
Also, the implementation has been tested throughly against the test suite adapted from MIT 6.824 course.

3. Note, that there is no main function. Instead, all the initilization are done through the tester code.

4. To run 
	export GOPATH=[installationpath]/Paxos_Lab
	a) paxos peer: 
		cd paxos  
		go test
	b) paxos peer along with key-value server
		cd paxos_kv
		go test

5. Tests are in test_test.go

6. Folder Structure
	/paxos_kv - contains code for key-value server and clients(application code)
	/paxos - contains code for paxos peer

7. To turn on debugging 
	For paxos peer set Debug flag in paxos/util.go
	For key-value server/client in paxos_kv/server.go 




#################################################################

RAFT

#################################################################
1. This folder contains implementation of Raft Protocol as described in the paper
"In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout

2. The implementation is for simulated network where clients and servers are just
separate go routines inside one process, and communicate using RPCs implemented over go channels.
Also, the implementation has been tested throughly against the test suite adapted from MIT 6.824 course.

3. Note, that there is not main function. Instead, all the initilization are done through the tester code.

4. The code implements the basic Raft protocol, but not the log compaction or cluster membership changes

5. To run 
	export GOPATH=[installationpath]/Raft_Lab
	a) raft peer: 
		cd raft  
		go test
	b) raft peer along with key-value server
		cd raft_kv
		go test

6. Tests are in test_test.go

7. Folder Structure
	/raft_kv - contains code for key-value server and clients(application code)
	/raft - contains code for raft peer

8. To turn on debugging 
	For raft peer set Debug flag in raft/util.go
	For key-value server/client in raft_kv/server.go 





