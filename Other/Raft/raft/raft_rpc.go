package raft

import (
	"fmt"

	//	"golang.org/x/net/context"
	//	"google.golang.org/grpc"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*const (
	RPCTimeout       = 2000
	ClientRPCTimeout = 10000
	DialTimeout      = 100
)*/

//RPC client connection map
//var rClientsMap = make(map[string]*rClient)
var replicaConn = make(map[string]*rpc.Client)
var replicaConnRetry = make(map[string]int)

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
func JoinRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	fmt.Printf("Join RPC Enter")
	req := JoinRequest{RemoteNode: remoteNode, FromNode: fromNode}
	reply := new(JoinReply)
	err := doDialCloseRemoteCall(remoteNode, "JoinRPC", &req, &reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to join cluster")
	}

	fmt.Printf("Join RPC Exit")
	return nil
}

//StartRPC
func StartRPC(remoteNode *NodeAddr, otherNodes []*NodeAddr) error {
	fmt.Printf("Start RPC Enter")
	req := StartRequest{}
	req.RemoteNode = remoteNode
	nodes := make([]*NodeAddr, len(otherNodes))
	for i, n := range otherNodes {
		nodes[i] = n
	}
	req.OtherNodes = nodes
	reply := new(StartReply)
	err := doDialCloseRemoteCall(remoteNode, "StartRPC", &req, &reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to Start")
	}

	fmt.Printf("Start RPC Exit")
	return err
}

// RequestVote RPC
//func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
func (r *RaftNode) RequestVoteRPC(remoteNode *NodeAddr, req RequestVoteArgs) (*RequestVoteReply, error) {
	/*	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("ReqVOTE send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	reply := new(RequestVoteReply)
	err := r.doRemoteCall(remoteNode, "RequestVoteRPC", &req, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

// Append Entries RPC
//func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
	/*r.INF("AppendEntriesRPC Enter")
	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("AppEntries send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	reply := new(AppendEntriesReply)
	err := r.doRemoteCall(remoteNode, "AppendEntriesRPC", &req, &reply, RPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Register Client
func (rc *RaftClient) ClientRegisterRPC(remoteNode *NodeAddr, request ClientRegisterArgs) (*ClientRegisterReply, error) {
	reply := new(ClientRegisterReply)
	err := rc.doRemoteCall(remoteNode, "ClientRegisterRequestRPC", &request, &reply, ClientRPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

//Request
func (rc *RaftClient) ClientRequestRPC(remoteNode *NodeAddr, request ClientRequestArgs) (*ClientReply, error) {
	reply := new(ClientReply)
	err := rc.doRemoteCall(remoteNode, "ClientRequestRPC", &request, &reply, ClientRPCTimeout)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

//GetTerm
func GetTermRPC(remoteNode *NodeAddr) (int32, error) {
	request := GetTermRequest{RemoteNode: remoteNode}
	reply := new(GetTermReply)

	err := makeRemoteCall(remoteNode, "GetTermRPC", &request, &reply, 600)
	return reply.Term, err
}

//GetState
func GetStateRPC(remoteNode *NodeAddr) (RaftState, error) {
	request := GetStateRequest{RemoteNode: remoteNode}
	reply := new(GetStateReply)

	err := makeRemoteCall(remoteNode, "GetStateRPC", &request, &reply, 600)
	return reply.State, err
}

//Enable Node
func EnableNodeRPC(remoteNode *NodeAddr) error {
	request := EnableNodeRequest{}
	reply := new(EnableNodeReply)
	err := makeRemoteCall(remoteNode, "EnableNodeRPC", &request, &reply, 600)
	return err
}

//Disable Node
func DisableNodeRPC(remoteNode *NodeAddr) error {
	request := DisableNodeRequest{}
	reply := new(DisableNodeReply)
	err := makeRemoteCall(remoteNode, "DisableNodeRPC", &request, &reply, 600)
	return err
}

//SetNodetoNode
func SetNodetoNodeRPC(remoteNode *NodeAddr, n NodeAddr, val bool) error {
	request := SetNodetoNodeRequest{ToNode: &n, Enable: val}
	reply := new(SetNodetoNodeReply)
	err := makeRemoteCall(remoteNode, "SetNodetoNodeRPC", &request, &reply, 600)
	return err
}
func (rc *RaftClient) doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	c := rc.Conns[*remoteAddr]
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

func (r *RaftNode) doRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	c := r.conns[*remoteAddr]
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

//makeRemoteCall
func makeRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
	var err error
	client, ok := replicaConn[remoteAddr.Addr]
	if !ok {
		if replicaConnRetry[remoteAddr.Addr] > 5 {
			fmt.Fprintf(os.Stderr, "Dial MaxRetry for: %s\n", remoteAddr.Addr)
			return fmt.Errorf("Dial MaxRetry. Server Dead\n")
		} else {
			//client, err = rpc.Dial("unix", remoteAddr.Addr)
			client, err = rpc.Dial("tcp4", remoteAddr.Addr)
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
	//	err = client.Call(fullProcName, request, reply)
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
