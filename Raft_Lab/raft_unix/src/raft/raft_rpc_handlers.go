//Name: raft_rpc_handlers.go
//Description: Contains RPC function that are invoked by the server in response to a RPC call
//Author: Rahul Batra

package raft

//RequestVoteMsg structure to wrap up an incoming RPC msg
type RequestVoteMsg struct {
	args  RequestVoteArgs
	reply chan RequestVoteReply
}

// RequestVote RPC handler.
//
//This receives an incoming RPC message and packages it into RequestVoteMsg structure.
//It then forwards to the run_server go routine through requestVoteMsgCh. And waits
//on replyCh before responsed back to the callee server
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	DPrintf("Serv[%d], RequestVote Hdl Enter\n", rf.me)
	replyCh := make(chan RequestVoteReply)
	rf.requestVoteMsgCh <- RequestVoteMsg{args, replyCh}
	*reply = <-replyCh
	DPrintf("Serv[%d], RequestVote Hdl Exit\n", rf.me)
	return nil
}

//AppendEntriesMsg structure to wrap up an incoming RPC msg
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
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	DPrintf("Serv[%d], AppendEntries Hdl Enter\n", rf.me)
	replyCh := make(chan AppendEntriesReply)
	rf.appendEntriesMsgCh <- AppendEntriesMsg{args, replyCh}
	*reply = <-replyCh
	DPrintf("Serv[%d], AppendEntries Hdl Exit\n", rf.me)
	return nil
}
