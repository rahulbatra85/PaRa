package paxos

import (
	"fmt"
	"net/rpc"
)

//RPC client connection map
var clientConn = make(map[string]*rpc.Client)

/////////////////////////////////////////
//JoinRPC
/////////////////////////////////////////
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

/////////////////////////////////////////
//StartRPC
/////////////////////////////////////////
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

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

/////////////////////////////////////////
//GetState
/////////////////////////////////////////
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

/////////////////////////////////////////
//Enable Node
/////////////////////////////////////////

/////////////////////////////////////////
//Disable Node
/////////////////////////////////////////

/////////////////////////////////////////
//SetSend
/////////////////////////////////////////

/////////////////////////////////////////
//SetReceive
/////////////////////////////////////////
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
