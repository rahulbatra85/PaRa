package raft

import (
	"fmt"
	"net/rpc"
)

//RPC client connection map
var replicaConn = make(map[string]*rpc.Client)

//JoinRPC
type JoinRequest struct {
	RemoteNode NodeAddr
	FromAddr   NodeAddr
}
type JoinReply struct {
	Success bool
}

func JoinRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	req := JoinRequest{RemoteNode: *remoteNode, FromAddr: *fromNode}
	var reply JoinReply
	err := makeRemoteCall(remoteNode, "JoinWrapper", req, &reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to join cluster")
	}

	return err
}

//StartRPC
type StartRequest struct {
	RemoteNode NodeAddr
	OtherNodes []NodeAddr
}

type StartReply struct {
	Success bool
}

func StartRPC(remoteNode *NodeAddr, otherNodes []NodeAddr) error {
	req := StartRequest{}
	req.RemoteNode = *remoteNode
	req.OtherNodes = make([]NodeAddr, len(otherNodes))
	for i, n := range otherNodes {
		req.OtherNodes[i].Id = n.Id
		req.OtherNodes[i].Addr = n.Addr
	}
	var reply StartReply
	err := makeRemoteCall(remoteNode, "StartWrapper", req, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return fmt.Errorf("Unable to start node")
	}

	return err
}

// RequestVote RPC
type RequestVoteArgs struct {
	Term        int
	CandidateId string
	LastLogIdx  int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (r *RaftNode) RequestVoteRPC(remoteNode *NodeAddr, req RequestVoteArgs, reply *RequestVoteReply) error {
	err := makeRemoteCall(remoteNode, "RequestVoteWrapper", req, reply)
	return err
}

// Append Entries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs, reply *AppendEntriesReply) error {
	err := makeRemoteCall(remoteNode, "AppendEntriesWrapper", req, reply)
	return err
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Request
type ClientRequest struct {
	Cmd Command
}

type ClientReply struct {
	Success bool
}

func ClientRequestRPC(remoteNode *NodeAddr, request ClientRequest) (*ClientReply, error) {
	var reply ClientReply
	err := makeRemoteCall(remoteNode, "ClientRequestWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

//GetTerm
type GetTermRequest struct {
	RemoteNode NodeAddr
}

type GetTermReply struct {
	Term    int
	Success bool
}

func GetTermRPC(remoteNode *NodeAddr) (error, int) {
	req := GetTermRequest{RemoteNode: *remoteNode}

	var reply GetTermReply
	err := makeRemoteCall(remoteNode, "GetTermWrapper", req, &reply)
	if err != nil {
		return err, reply.Term
	}
	if !reply.Success {
		return fmt.Errorf("Unable to get state"), reply.Term
	}

	return err, reply.Term
}

//GetState
type GetStateRequest struct {
	RemoteNode NodeAddr
}

type GetStateReply struct {
	State   RaftState
	Success bool
}

func GetStateRPC(remoteNode *NodeAddr) (error, RaftState) {
	req := GetStateRequest{RemoteNode: *remoteNode}

	var reply GetStateReply
	err := makeRemoteCall(remoteNode, "GetStateWrapper", req, &reply)
	if err != nil {
		return err, reply.State
	}
	if !reply.Success {
		return fmt.Errorf("Unable to get state"), reply.State
	}

	return err, reply.State
}

//Enable Node

//Disable Node

//SetSend

//SetReceive

//makeRemoteCall
func makeRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}) error {
	var err error
	client, ok := replicaConn[remoteAddr.Addr]
	if !ok {
		client, err = rpc.Dial("tcp", remoteAddr.Addr)
		if err != nil {
			return err
		}
		replicaConn[remoteAddr.Addr] = client
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	err = client.Call(fullProcName, request, reply)
	if err != nil {
		delete(replicaConn, remoteAddr.Addr)
	}
	return err

}
