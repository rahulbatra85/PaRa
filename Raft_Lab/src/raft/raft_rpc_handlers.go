package raft

//
//RequestVoteMsg structure to wrap up an incoming RPC msg
//
type RequestVoteMsg struct {
	args  RequestVoteArgs
	reply chan RequestVoteReply
}

//
// RequestVote RPC handler.
//
//This receives an incoming RPC message and packages it into RequestVoteMsg structure.
//It then forwards to the run_server go routine through requestVoteMsgCh. And waits
//on replyCh before responsed back to the callee server
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Serv[%d], Request Vote Enter\n", rf.me)
	replyCh := make(chan RequestVoteReply)
	rf.requestVoteMsgCh <- RequestVoteMsg{args, replyCh}
	*reply = <-replyCh
	DPrintf("Serv[%d], Request Vote Exit\n", rf.me)
	return
}

//
//AppendEntriesMsg structure to wrap up an incoming RPC msg
//
type AppendEntriesMsg struct {
	args  AppendEntriesArgs
	reply chan AppendEntriesReply
}

//
// AppendEntries RPC handler.
//
//This receives an incoming RPC message and packages it into AppendEntriesMsg structure.
//It then forwards to the local "run_server" go routine through appendEntriesMsgCh . And waits
//on replyCh before responding back to the callee server
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan AppendEntriesReply)
	rf.appendEntriesMsgCh <- AppendEntriesMsg{args, replyCh}
	*reply = <-replyCh
	return
}
