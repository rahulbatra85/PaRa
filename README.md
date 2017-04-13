
#################################################################

PAXOS

#################################################################

1. This code contains implementation of Multi-Paxos Protocol as described 
in the paper "Paxos Made Simple" by Leslie Lamport

2. The implementation is for simulated network where clients and servers are 
just separate go routines inside one process, and communicate using RPCs 
implemented over unix sockets. Also, the implementation has been tested 
throughly against a comprehensive test suite adapted from MIT 6.824 course.

3. Note, that there is no main function. Instead, all the initilization is 
	done through the tester code.

4. To run 
	 export GOPATH=[pathprefix]/Paxos_Lab/
	a) paxos peer: 
		cd paxos  
		go test
	b) paxos peer along with key-value server
	   cd paxos_kv
	   go test
	c) paxos profile
		cd paxos_profiler
		go build paxos_profiler.go
		./paxos_profiler

5. Tests are in paxos/test_test.go and paxos_kv/test_test.go

6. Folder Structure
   	/paxos_kv - contains code for key-value server and clients(application code)
   	/paxos - contains code for paxos peer
	/paxos_profiler - contains code profiling paxos based key-value service

7. To turn on debugging 
	For paxos peer set Debug flag in paxos/util.go
	For key-value server/client in paxos_kv/server.go 


#################################################################

RAFT

#################################################################
1. This code contains implementation of Raft Protocol as described in the paper
"In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout

2. The implementation is for simulated network where clients and servers are just
separate go routines inside one process, and communicate using RPCs implemented over go channels(raft_labrpc or unix sockets(raft_unix). The code for different communication options appear under raft_unix and raft_labrpc. Note, that besides different of communication method, there is no different in the code. The algorithm implementation is same. However, currently the test suite is only compatible with raft_labrpc, and has been tested throughly against the test suite adapted from MIT 6.824 course.

3. Note, that there is no main function. Instead, all the initilization are done through the tester code.

4. The code implements the basic Raft protocol, but not the log compaction or cluster membership changes

4. To run 
	labrpc
		export GOPATH=[pathprefix]/Raft_Lab/raft_labrpc
		a) raft peer: 
			cd raft  
			go test
		b) raft peer along with key-value server
		   cd raft_kv
		   go test
	raft_unix
		export GOPATH=[pathprefix]/Raft_Lab/raft_unix
		a) cd raft_unix/raft_profiler
			go build raft_profiler.go
			./raft_profiler -p r(for running with reliable network)
			./raft_profiler -p u(for running with unreliable network)

5. Tests are in raft_labrpc/raft/test_test.go and raft_labrpc/raft_kv/test_test.go

6. Folder Structure
 	raft_labrpc- contains raft code where all communication is done with custom labrpc library
		/src
			/raft - contains code for raft node
			/raft_kv - contains code for raft based key-value server
			/raft_profiler - contains code for profiling raft based key-value implementation
	raft_unix - containt raft code where the communication is done with unix sockets
		/src
			/raft - contains code for raft node
			/raft_kv - contains code for raft based key-value server
			/raft_profiler - contains code for profiling raft based key-value implementation

7. To turn on debugging 
	For raft node set Debug flag in raft/util.go
	For key-value server/client in raft_kv/server.go 





