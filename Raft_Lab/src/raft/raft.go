//Name: raft.go
//Description: Main file for raft server
//Author: Rahul Batra(except empty functions adapted from MIT 6.824 course)

package raft

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"

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
	mu        sync.Mutex
	stmu      sync.RWMutex
	applyMu   sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialization code here.
	r := persister.ReadRaftState()
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
	rf.readPersist(r)
	if len(rf.Log) == 0 {
		entry := LogEntry{Term: 0, Cmd: nil}
		rf.AppendLog(entry)
	}

	DPrintf("Serv[%d] Initial Term=%d,VotedFor=%d,Log=%v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log)

	//Start Raft Server
	go rf.run_server()

	return rf
}
