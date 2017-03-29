package paxos

import (
	"fmt"
	"net/rpc"
	"os"
	"time"
)

const (
	RPCTimeout       = 100
	ClientRPCTimeout = 1000
)

//RPC client connection map
var replicaConn = make(map[string]*rpc.Client)
var replicaConnRetry = make(map[string]int)

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
	reply = new(JoinReply)
	err := doRemoteCall(remoteNode, "JoinWrapper", &req, &reply, RPCTimeout)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to join cluster")
	}

	return nil
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
	reply := new(StartReply)
	err := doRemoteCall(remoteNode, "StartWrapper", &req, &reply, RPCTimeout)
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
	reply = new(ProposeReply)
	err := doRemoteCall(remoteNode, "ProposeWrapper", request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
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
	reply := new(DecisionReply)
	err := doRemoteCall(remoteNode, "DecisionWrapper", request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
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
	reply := new(P1aReply)
	err := doRemoteCall(remoteNode, "P1aWrapper", request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
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
	reply := new(P2aReply)
	err := doRemoteCall(remoteNode, "P2aWrapper", request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
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
	reply := new(P1bReply)
	err := doRemoteCall(remoteNode, "P1bWrapper", request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
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
	reply := new(P2bReply)
	err := doRemoteCall(remoteNode, "P2bWrapper", &request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////
//Register
type ClientRegisterArgs struct {
	FromNode NodeAddr
}

type ClientRegisterReply struct {
	Code     ClientReplyCode
	ClientId int
}

//Register Client
func ClientRegisterRPC(remoteNode *NodeAddr, request ClientRegisterArgs) (*ClientRegisterReply, error) {
	reply := new(ClientRegisterReply)
	err := doRemoteCall(remoteNode, "ClientRegisterWrapper", &request, &reply, ClientRPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

//Request
type ClientRequestArgs struct {
	Cmd Command
}
type ClientReply struct {
	Code       ClientReplyCode
	LeaderNode *NodeAddr
	Value      string
	SeqNum     int
}

//Request
func ClientRequestRPC(remoteNode *NodeAddr, request ClientRequestArgs) (*ClientReply, error) {
	reply := new(ClientReply)
	err := doRemoteCall(remoteNode, "ClientRequestWrapper", &request, &reply, ClientRPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

//doRemoteCall
func doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	var err error
	client, ok := replicaConn[remoteAddr.Addr]
	if !ok {
		if replicaConnRetry[remoteAddr.Addr] > 5 {
			fmt.Fprintf(os.Stderr, "Dial MaxRetry for: %s\n", remoteAddr.Addr)
			return fmt.Errorf("Dial MaxRetry. Server Dead\n")
		} else {
			client, err = rpc.Dial("unix", remoteAddr.Addr)
			//client, err = rpc.Dial("tcp4", remoteAddr.Addr)
			if err != nil {
				replicaConnRetry[remoteAddr.Addr] = replicaConnRetry[remoteAddr.Addr] + 1
				fmt.Fprintf(os.Stderr, "Dial Failed: %v\n", err)
				return err
			}
			replicaConnRetry[remoteAddr.Addr] = 0
			replicaConn[remoteAddr.Addr] = client
		}
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	call := client.Go(fullProcName, request, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			fmt.Fprintf(os.Stderr, "Client Call Failed: %v\n", err)
			client.Close()
			delete(replicaConn, remoteAddr.Addr)
			return call.Error
		} else {
			return nil
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		fmt.Fprintf(os.Stderr, "Client Call Timeout: %v\n", err)
		client.Close()
		delete(replicaConn, remoteAddr.Addr)
		return fmt.Errorf("Client call Timeout")
	}

}
