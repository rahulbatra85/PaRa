//Name: raft.go
//Description: Main file for raft server
//Author: Rahul Batra(except empty functions adapted from MIT 6.824 course)

package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type RaftState int

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu         sync.Mutex
	stmu       sync.RWMutex
	applyMu    sync.Mutex
	persister  *Persister
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Raft paper's Figure 2 description of state
	state     RaftState
	nextState RaftState

	//Persistent State on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	//Volatile state on all servers
	commitIndex int64
	lastApplied int

	//Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	//channels
	appendEntriesMsgCh   chan AppendEntriesMsg
	requestVoteMsgCh     chan RequestVoteMsg
	appendEntriesReplyCh chan AppendEntriesReply
	requestVoteReplyCh   chan RequestVoteReply
	applyMsgCh           chan ApplyMsg

	//config
	heartbeatFrequency int //in ms
	electionTimeout    int //in ms
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// call() will time out and return an error after a while
//if it does not get a reply from the server.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// Returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.getCurrentTerm()
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// Saves Raft's persistent state to stable storage,
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("Serv[%d] Persist\n", rf.me)
}

//
// Restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	var term int
	var votedFor int
	var log []LogEntry
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&term)
	d.Decode(&votedFor)
	d.Decode(&log)
	rf.CurrentTerm = term
	rf.VotedFor = votedFor
	rf.Log = log
	DPrintf("Serv[%d] readPersist Term=%d,VotedFor=%d,Log=%v\n", rf.me, term, votedFor, log)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, isLeader := rf.AppendLogLeader(command)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. Not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	//do nothing
	atomic.StoreInt32(&rf.dead, 1)
	if rf.l != nil {
		rf.l.Close()
	}
}

// has this peer been asked to shut down?
func (rf *Raft) isdead() bool {
	return atomic.LoadInt32(&rf.dead) != 0
}

//Used for testing
func (rf *Raft) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&rf.unreliable, 1)
	} else {
		atomic.StoreInt32(&rf.unreliable, 0)
	}
}

//Used for testing
func (rf *Raft) isunreliable() bool {
	return atomic.LoadInt32(&rf.unreliable) != 0
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. Persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []string, me int,
	persister *Persister, applyCh chan ApplyMsg, rpcs *rpc.Server) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.state = FOLLOWER
	rf.heartbeatFrequency = 175
	rf.electionTimeout = 600

	//Channels
	rf.appendEntriesMsgCh = make(chan AppendEntriesMsg)
	rf.requestVoteMsgCh = make(chan RequestVoteMsg)
	rf.appendEntriesReplyCh = make(chan AppendEntriesReply)
	rf.requestVoteReplyCh = make(chan RequestVoteReply)
	rf.applyMsgCh = applyCh

	// initialize from persisted state
	DPrintf("Serv[%d] Starting Raft. Loading Persistent State\n", rf.me)
	rf.CurrentTerm = 0
	rf.VotedFor = 0
	if len(rf.Log) == 0 {
		entry := LogEntry{Term: 0, Cmd: nil}
		rf.AppendLog(entry)
	}

	DPrintf("Serv[%d] Initial Term=%d,VotedFor=%d,Log=%v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(rf)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(rf)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		rf.l = l

		// create a thread to accept RPC connections
		go func() {
			for rf.isdead() == false {
				conn, err := rf.l.Accept()
				if err == nil && rf.isdead() == false {
					if rf.isunreliable() && (rand.Int63()%2000) < 100 {
						// discard the request.
						conn.Close()
					} else if rf.isunreliable() && (rand.Int63()%2000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&rf.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&rf.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && rf.isdead() == false {
					fmt.Printf("Raft(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	//Start Raft Server
	go rf.run_server()

	return rf
}
