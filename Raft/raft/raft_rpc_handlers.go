package raft

import (
	"fmt"
)

//JoinRPC Handler
func (r *RaftNode) Join(request *JoinRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	//Received a Join Request, so append to othersAddr list
	if len(r.othersAddr) == r.config.ClusterSize {
		return fmt.Errorf("Node tried to join after all node have already joined")
	} else {
		r.othersAddr = append(r.othersAddr, request.FromNode)
		r.INF("JoinRPC from %s %s", request.FromNode.Id, request.FromNode.Addr)
	}

	return nil
}

//StartRPC Handler
func (r *RaftNode) Start(request *StartRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	//Set OthersAddr list
	r.INF("Received START")
	for _, node := range request.OtherNodes {
		r.othersAddr = append(r.othersAddr, node)
	}

	r.INF("OtherNode=%v", r.othersAddr)
	/*	if r.nodeMgrAddr.Id != "" && r.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&r.nodeMgrAddr, &r.localAddr)
	}*/

	//Start Server
	go r.run_server()

	return nil
}

//
// RequestVote RPC handler.
//
//RequestVoteMsg structure to wrap up an incoming RPC msg
type RequestVoteMsg struct {
	args  *RequestVoteArgs
	reply chan RequestVoteReply
}

//This receives an incoming RPC message and packages it into RequestVoteMsg structure.
//It then forwards to the run_server go routine through requestVoteMsgCh. And waits
//on replyCh before responding back to the callee server
func (r *RaftNode) RequestVote(args *RequestVoteArgs) (RequestVoteReply, error) {
	r.INF("ReqVote Hdl Enter")
	replyCh := make(chan RequestVoteReply)
	r.requestVoteMsgCh <- RequestVoteMsg{args, replyCh}
	r.INF("ReqVote Hdl Exit")
	return <-replyCh, nil
}

// AppendEntries RPC handler.
//
//AppendEntriesMsg structure to wrap up an incoming RPC msg
type AppendEntriesMsg struct {
	args  *AppendEntriesArgs
	reply chan AppendEntriesReply
}

//
//This receives an incoming RPC message and packages it into AppendEntriesMsg structure.
//It then forwards to the local "run_server" go routine through appendEntriesMsgCh . And waits
//on replyCh before responding back to the callee server
func (r *RaftNode) AppendEntries(args *AppendEntriesArgs) (AppendEntriesReply, error) {
	r.INF("Append Entries Hdl Enter")
	replyCh := make(chan AppendEntriesReply)
	r.appendEntriesMsgCh <- AppendEntriesMsg{args, replyCh}
	r.INF("Append Entries Hdl Exit")
	return <-replyCh, nil
}

// Client Register RPC handler.
//
//ClientRegisterMSG structure to wrap up an incoming RPC msg
type ClientRegisterMsg struct {
	args  *ClientRegisterArgs
	reply chan ClientRegisterReply
}

func (r *RaftNode) ClientRegister(args *ClientRegisterArgs) (ClientRegisterReply, error) {
	r.INF("ClientRegister Enter")
	replyCh := make(chan ClientRegisterReply)
	r.clientRegisterMsgCh <- ClientRegisterMsg{args, replyCh}
	r.INF("ClientRegister Exit")
	return <-replyCh, nil
}

// Client Request RPC handler.
//
//ClientRequest MSG structure to wrap up an incoming RPC msg
type ClientRequestMsg struct {
	args  *ClientRequestArgs
	reply chan ClientReply
}

func (r *RaftNode) ClientRequest(args *ClientRequestArgs) (ClientReply, error) {
	r.INF("ClientRequest Enter")
	replyCh := make(chan ClientReply)
	r.clientRequestMsgCh <- ClientRequestMsg{args, replyCh}
	r.INF("ClientRequest Exit")
	return <-replyCh, nil
}

//GetTerm
func (r *RaftNode) GetTerm(req *GetTermRequest) (GetTermReply, error) {
	reply := GetTermReply{Success: true, Term: r.getCurrentTerm()}

	return reply, nil
}

//GetState
func (r *RaftNode) GetState(req *GetStateRequest) (GetStateReply, error) {
	reply := GetStateReply{Success: true, State: r.getState()}
	r.INF("State=%d", reply.State)
	return reply, nil
}

//Enable Node
func (r *RaftNode) EnableNode(req *EnableNodeRequest) (EnableNodeReply, error) {
	reply := EnableNodeReply{Success: true}
	r.netConfig.EnableNetwork()

	return reply, nil
}

//Disable Node
func (r *RaftNode) DisableNode(req *DisableNodeRequest) (DisableNodeReply, error) {
	reply := DisableNodeReply{Success: true}
	r.netConfig.DisableNetwork()

	return reply, nil
}

//SetNodetoNode
func (r *RaftNode) SetNodetoNode(req *SetNodetoNodeRequest) (SetNodetoNodeReply, error) {
	reply := SetNodetoNodeReply{Success: true}
	r.netConfig.SetNetworkConfig(r.localAddr, *(req.ToNode), req.Enable)

	return reply, nil
}
