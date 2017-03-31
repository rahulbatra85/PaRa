package paxos

import (
	"fmt"
	"net"
	"net/rpc"
	//"os"
	"sync"
	"time"
)

const (
	RPCTimeout       = 1000
	ClientRPCTimeout = 10000
	DialTimeout      = 100
)

//RPC client connection map
//var replicaConn = make(map[string]*rpc.Client)
//var replicaConnRetry = make(map[string]int)
//var RPCMu sync.Mutex

type connection struct {
	rpcClient  *rpc.Client
	conn       net.Conn
	mu         sync.Mutex
	connClosed bool
}

func MakeConnection(remoteAddr *NodeAddr) *connection {
	c := new(connection)
	var err error
	c.conn, err = net.DialTimeout("tcp4", remoteAddr.Addr, time.Duration(DialTimeout)*time.Millisecond)
	if err != nil {
		return nil
	}
	c.rpcClient = rpc.NewClient(c.conn)
	c.connClosed = false
	return c
}

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
	reply := new(JoinReply)
	err := doDialCloseRemoteCall(remoteNode, "JoinWrapper", &req, &reply)
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
	err := doDialCloseRemoteCall(remoteNode, "StartWrapper", &req, &reply)
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
	p.INF("ProposeRPC ")
	reply := new(ProposeReply)
	err := p.doRemoteCall(remoteNode, "ProposeWrapper", request, &reply, RPCTimeout)
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
	p.INF("DecisionRPC ")
	reply := new(DecisionReply)
	err := p.doRemoteCall(remoteNode, "DecisionWrapper", request, &reply, RPCTimeout)
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
	err := p.doRemoteCall(remoteNode, "P1aWrapper", request, &reply, RPCTimeout)
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
	err := p.doRemoteCall(remoteNode, "P2aWrapper", request, &reply, RPCTimeout)
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
	Rval     map[int]Pvalue
}

type P1bReply struct {
	Success bool
}

func (p *PaxosNode) P1bRPC(remoteNode *NodeAddr, request P1bRequest) (*P1bReply, error) {
	reply := new(P1bReply)
	err := p.doRemoteCall(remoteNode, "P1bWrapper", request, &reply, RPCTimeout)
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
	err := p.doRemoteCall(remoteNode, "P2bWrapper", &request, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, err
	}
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Request
type ClientRequestArgs struct {
	Cmd      Command
	FromNode NodeAddr
}
type ClientReply struct {
	Code   ClientReplyCode
	Value  string
	SeqNum int
}

//Request
func (kvc *PaxosClientKV) ClientRequestRPC(remoteNode *NodeAddr, request ClientRequestArgs) (*ClientReply, error) {
	reply := new(ClientReply)
	err := kvc.doRemoteCall(remoteNode, "ClientRequestWrapper", &request, &reply, ClientRPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

func (kvc *PaxosClientKV) doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	c := kvc.Conns[*remoteAddr]
	//Only one RPC per outgoing connection
	//c.mu.Lock()
	//defer c.mu.Unlock()
	var err error
	if c.connClosed {
		c.conn, err = net.DialTimeout("tcp4", remoteAddr.Addr, time.Duration(DialTimeout)*time.Millisecond)
		if err != nil {
			return err
		}
		c.rpcClient = rpc.NewClient(c.conn)
		c.connClosed = false
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	c.conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
	err = c.rpcClient.Call(fullProcName, request, reply)
	if err != nil {
		c.rpcClient.Close()
		c.connClosed = true
		return err
	}

	return nil
}

func (p *PaxosNode) doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	c := p.conns[*remoteAddr]
	//Only one RPC per outgoing connection
	//c.mu.Lock()
	//defer c.mu.Unlock()
	var err error
	if c.connClosed {
		c.conn, err = net.DialTimeout("tcp4", remoteAddr.Addr, time.Duration(DialTimeout)*time.Millisecond)
		if err != nil {
			return err
		}
		c.rpcClient = rpc.NewClient(c.conn)
		c.connClosed = false
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	c.conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
	err = c.rpcClient.Call(fullProcName, request, reply)
	if err != nil {
		c.rpcClient.Close()
		c.connClosed = true
		return err
	}

	return nil
}

func doDialCloseRemoteCall(remoteNode *NodeAddr, procName string, request interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", remoteNode.Addr)
	if err != nil {
		return err
	}

	// Make the request
	fullProcName := fmt.Sprintf("%v.%v", remoteNode.Addr, procName)
	err = client.Call(fullProcName, request, reply)
	client.Close()

	return err
}

/*
//doRemoteCall
func doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	var err error
	RPCMu.Lock()
	client, ok := replicaConn[remoteAddr.Addr]
	RPCMu.Unlock()
	if !ok {
		//client, err = rpc.Dial("unix", remoteAddr.Addr)
		client, err = rpc.Dial("tcp4", remoteAddr.Addr)
		if err != nil {
			replicaConnRetry[remoteAddr.Addr] = replicaConnRetry[remoteAddr.Addr] + 1
			fmt.Fprintf(os.Stderr, "Dial Failed: %v\n", err)
			return err
		}
		RPCMu.Lock()
		replicaConnRetry[remoteAddr.Addr] = 0
		replicaConn[remoteAddr.Addr] = client
		RPCMu.Unlock()

	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	call := client.Go(fullProcName, request, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			fmt.Fprintf(os.Stderr, "Client Call Failed: %v, %s\n", remoteAddr, procName)
			client.Close()
			RPCMu.Lock()
			delete(replicaConn, remoteAddr.Addr)
			RPCMu.Unlock()
			return call.Error
		} else {
			return nil
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		fmt.Fprintf(os.Stderr, "Client Call Timeout: %v\n", err)
		client.Close()
		RPCMu.Lock()
		delete(replicaConn, remoteAddr.Addr)
		RPCMu.Unlock()
		return fmt.Errorf("Client call Timeout")
	}

}*/
