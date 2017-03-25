package raft

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"net"
	//"net/rpc"
	//"time"
)

//RPC client connection map
var replicaClient = make(map[string]RaftRPCClient)
var replicaConn = make(map[string]*grpc.ClientConn)

//JoinRPC
func JoinRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	req := JoinRequest{RemoteNode: remoteNode, FromNode: fromNode}
	client, err := getClient(remoteNode)
	if err != nil {
		return err
	}
	reply, err := client.JoinRPC(context.Background(), &req)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to join cluster")
	}

	return err
}

//StartRPC
func StartRPC(remoteNode *NodeAddr, otherNodes []*NodeAddr) error {
	req := StartRequest{}
	req.RemoteNode = remoteNode
	nodes := make([]*NodeAddr, len(otherNodes))
	for i, n := range otherNodes {
		nodes[i] = n
	}
	req.OtherNodes = nodes
	client, err := getClient(remoteNode)
	if err != nil {
		return err
	}
	reply, err := client.StartRPC(context.Background(), &req)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to Start")
	}

	return err
}

// RequestVote RPC
func (r *RaftNode) RequestVoteRPC(remoteNode *NodeAddr, req RequestVoteArgs) (*RequestVoteReply, error) {
	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("ReqVOTE send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	return client.RequestVoteRPC(context.Background(), &req)
}

// Append Entries RPC
func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
	r.INF("AppendEntriesRPC Enter")
	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("AppEntries send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	return client.AppendEntriesRPC(context.Background(), &req, grpc.FailFast(true))
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Request
func ClientRequestRPC(remoteNode *NodeAddr, request ClientRequest) (*ClientReply, error) {
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	return client.ClientRequestRPC(context.Background(), &request)
}

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

//GetTerm
func GetTermRPC(remoteNode *NodeAddr) (int32, error) {
	request := GetTermRequest{RemoteNode: remoteNode}

	client, err := getClient(remoteNode)
	if err != nil {
		return 0, err
	} else {
		reply, err := client.GetTermRPC(context.Background(), &request)
		return reply.Term, err
	}
}

//GetState
func GetStateRPC(remoteNode *NodeAddr) (RaftState, error) {
	request := GetStateRequest{RemoteNode: remoteNode}

	client, err := getClient(remoteNode)
	if err != nil {
		return 0, err
	} else {
		reply, err := client.GetStateRPC(context.Background(), &request)
		return reply.State, err
	}
}

//Enable Node
func EnableNodeRPC(remoteNode *NodeAddr) error {
	request := EnableNodeRequest{}

	client, err := getClient(remoteNode)
	if err != nil {
		return err
	}
	_, err = client.EnableNodeRPC(context.Background(), &request)
	return err

}

//Disable Node
func DisableNodeRPC(remoteNode *NodeAddr) error {
	request := DisableNodeRequest{}

	client, err := getClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.DisableNodeRPC(context.Background(), &request)
	return err
}

//SetNodetoNode
func SetNodetoNodeRPC(remoteNode *NodeAddr, n NodeAddr, val bool) error {
	request := SetNodetoNodeRequest{ToNode: &n, Enable: val}

	client, err := getClient(remoteNode)
	if err != nil {
		return err
	}
	_, err = client.SetNodetoNodeRPC(context.Background(), &request)
	return err
}

func getClient(remoteAddr *NodeAddr) (RaftRPCClient, error) {
	client, ok := replicaClient[remoteAddr.Addr]
	if !ok {
		conn, err := grpc.Dial(remoteAddr.Addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		client = NewRaftRPCClient(conn)
		replicaClient[remoteAddr.Addr] = client
		replicaConn[remoteAddr.Addr] = conn
	}
	return client, nil
}

func closeClient(remoteAddr *NodeAddr) {
	//	client, ok := replicaClient[remoteAddr.Addr]

}
