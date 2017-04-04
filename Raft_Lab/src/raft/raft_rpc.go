package raft

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
//Raft RPC API for sendRequestVote
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("Serv[%d], SendRequestVote to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Serv[%d], SendRequestVote rsp from %d\n", rf.me, server)
	return ok
}

//
// Append Entries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
//  AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
//Raft RPC API for appendEntries
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Serv[%d], SendAppendEntries to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("Serv[%d], SendAppendEntries rsp from %d\n", rf.me, server)
	return ok
}
