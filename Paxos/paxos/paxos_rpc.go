package paxos

import (
	"fmt"
	"net/rpc"
)

//RPC client connection map
var clientConn = make(map[string]*rpc.Client)

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

//Propose
type ProposeRequest struct {
	Slot int
	Cmd  Command
}

type ProposeReply struct {
	Success bool
}

func (p *PaxosNode) ProposeRPC(remoteNode *NodeAddr, request ProposeRequest) (*ProposeReply, error) {
	//\todo add enable check
	var reply ProposeReply
	err := makeRemoteCall(remoteNode, "ProposeWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

//Decision
type DecisionRequest struct {
	Slot int
	Cmd  Command
}

type DecisionReply struct {
	Success bool
}

func (p *PaxosNode) DecisionRPC(remoteNode *NodeAddr, request DecisionRequest) (*DecisionReply, error) {
	var reply DecisionReply
	err := makeRemoteCall(remoteNode, "DecisionWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

//P1a
type P1aRequest struct {
	ScoutId string
	Leader  NodeAddr
	Bnum    BallotNum
}

type P1aReply struct {
	Success bool
}

func (p *PaxosNode) P1aRPC(remoteNode *NodeAddr, request P1aRequest) (*P1aReply, error) {
	var reply P1aReply
	err := makeRemoteCall(remoteNode, "P1aWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

//P2a
type P2aRequest struct {
	CommanderId string
	Leader      NodeAddr
	Pval        Pvalue
}

type P2aReply struct {
	Success bool
}

func (p *PaxosNode) P2aRPC(remoteNode *NodeAddr, request P2aRequest) (*P2aReply, error) {
	var reply P2aReply
	err := makeRemoteCall(remoteNode, "P2aWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

//P1b
type P1bRequest struct {
	ScoutId  string
	Acceptor NodeAddr
	Bnum     BallotNum
	Rval     Pvalue
}

type P1bReply struct {
	Success bool
}

func (p *PaxosNode) P1bRPC(remoteNode *NodeAddr, request P1bRequest) (*P1bReply, error) {
	var reply P1bReply
	err := makeRemoteCall(remoteNode, "P1bWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

//P2b
type P2bRequest struct {
	CommanderId string
	Acceptor    NodeAddr
	Bnum        BallotNum
}

type P2bReply struct {
	Success bool
}

func (p *PaxosNode) P2bRPC(remoteNode *NodeAddr, request P2bRequest) (*P2bReply, error) {
	var reply P2bReply
	err := makeRemoteCall(remoteNode, "P2bWrapper", request, &reply)
	if err != nil {
		return nil, err
	}

	return &reply, err
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////
//Register
type RegisterClientRequest struct {
}

type RegisterClientReply struct {
}

//Request
type ClientRequest struct {
}

type ClientReply struct {
}

//Response

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

//GetState
type GetStateRequest struct {
	RemoteNode NodeAddr
	FromAddr   NodeAddr
}

type GetStateReply struct {
	Success bool
	State   int
}

func GetStateRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	req := GetStateRequest{RemoteNode: *remoteNode, FromAddr: *fromNode}

	var reply GetStateReply
	err := makeRemoteCall(remoteNode, "GetStateWrapper", req, &reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to get state")
	}

	return err
}

//Enable Node

//Disable Node

//SetSend

//SetReceive

//makeRemoteCall
func makeRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}) error {
	var err error
	client, ok := clientConn[remoteAddr.Addr]
	if !ok {
		client, err = rpc.Dial("tcp", remoteAddr.Addr)
		if err != nil {
			return err
		}
		clientConn[remoteAddr.Addr] = client
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	err = client.Call(fullProcName, request, reply)
	if err != nil {
		delete(clientConn, remoteAddr.Addr)
	}
	return err

}
