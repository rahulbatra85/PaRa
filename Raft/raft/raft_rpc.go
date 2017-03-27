package raft

import (
	"fmt"

	//	"golang.org/x/net/context"
	//	"google.golang.org/grpc"
	//"net"
	"net/rpc"
	"os"
	"time"
)

const (
	RPCTimeout = 1500
)

//RPC client connection map
//var rClientsMap = make(map[string]*rClient)
var replicaConn = make(map[string]*rpc.Client)
var replicaConnRetry = make(map[string]int)

/*type rClient struct {
	rpcClient RaftRPCClient
	conn      *grpc.ClientConn
	cnt       int
}*/

//JoinRPC
func JoinRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	fmt.Printf("Join RPC Enter")
	req := JoinRequest{RemoteNode: remoteNode, FromNode: fromNode}
	reply := new(JoinReply)
	err := makeRemoteCall(remoteNode, "JoinRPC", &req, &reply, 600)
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
	err := makeRemoteCall(remoteNode, "StartRPC", &req, &reply, 600)
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
func RequestVoteRPC(remoteNode *NodeAddr, req RequestVoteArgs) (*RequestVoteReply, error) {
	/*	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("ReqVOTE send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	reply := new(RequestVoteReply)
	err := makeRemoteCall(remoteNode, "RequestVoteRPC", &req, &reply, 600)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

// Append Entries RPC
//func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
func AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
	/*r.INF("AppendEntriesRPC Enter")
	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("AppEntries send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	reply := new(AppendEntriesReply)
	err := makeRemoteCall(remoteNode, "AppendEntriesRPC", &req, &reply, 600)
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
func ClientRegisterRPC(remoteNode *NodeAddr, request ClientRegisterArgs) (*ClientRegisterReply, error) {
	reply := new(ClientRegisterReply)
	err := makeRemoteCall(remoteNode, "ClientRegisterRequestRPC", &request, &reply, 2000)
	if err != nil {
		return nil, err
	} else {
		return reply, nil
	}
}

//Request
func ClientRequestRPC(remoteNode *NodeAddr, request ClientRequestArgs) (*ClientReply, error) {
	reply := new(ClientReply)
	err := makeRemoteCall(remoteNode, "ClientRequestRPC", &request, &reply, 2000)
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

//makeRemoteCall
func makeRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}, timeout int) error {
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
