package raft

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"net"
	//"net/rpc"
	"time"
)

const (
	RPCTimeout = 1500
)

//RPC client connection map
var rClientsMap = make(map[string]*rClient)

//var replicaConn = make(map[string]*grpc.ClientConn)

type rClient struct {
	rpcClient RaftRPCClient
	conn      *grpc.ClientConn
	cnt       int
}

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
//func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
func RequestVoteRPC(remoteNode *NodeAddr, req RequestVoteArgs) (*RequestVoteReply, error) {
	/*	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("ReqVOTE send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	var err error
	var reply *RequestVoteReply
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(RPCTimeout))
	defer cancel()
	reply, err = client.RequestVoteRPC(ctx, &req)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// Append Entries RPC
//func (r *RaftNode) AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
func AppendEntriesRPC(remoteNode *NodeAddr, req AppendEntriesArgs) (*AppendEntriesReply, error) {
	/*r.INF("AppendEntriesRPC Enter")
	if r.netConfig.GetNetworkConfig(r.localAddr, *remoteNode) == false {
		r.INF("AppEntries send NOT allowed")
		return nil, fmt.Errorf("Not allowed")
	}*/
	var err error
	var reply *AppendEntriesReply
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(RPCTimeout))
	defer cancel()
	reply, err = client.AppendEntriesRPC(ctx, &req)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Register Client
func ClientRegisterRPC(remoteNode *NodeAddr, request ClientRegisterArgs) (*ClientRegisterReply, error) {
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(4000))
	defer cancel()
	return client.ClientRegisterRequestRPC(ctx, &request)
}

//Request
func ClientRequestRPC(remoteNode *NodeAddr, request ClientRequestArgs) (*ClientReply, error) {
	client, err := getClient(remoteNode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(4000))
	defer cancel()
	return client.ClientRequestRPC(ctx, &request)
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
	rc, ok := rClientsMap[remoteAddr.Addr]
	if !ok {
		conn, err := grpc.Dial(remoteAddr.Addr, grpc.WithInsecure(), grpc.WithTimeout(time.Millisecond*200))
		if err != nil {
			return nil, err
		}
		rc = &rClient{}
		rc.rpcClient = NewRaftRPCClient(conn)
		rc.conn = conn
		rc.cnt = 0
		rClientsMap[remoteAddr.Addr] = rc
	}
	/*else if rc.cnt > 25 {
		fmt.Printf("Closing Connection to %s", remoteAddr.Addr)
		rc.conn.Close()
		conn, err := grpc.Dial(remoteAddr.Addr, grpc.WithInsecure(), grpc.WithTimeout(time.Millisecond*200))
		if err != nil {
			return nil, err
		}
		rc.rpcClient = NewRaftRPCClient(conn)
		rc.conn = conn
		rc.cnt = 0
		rClientsMap[remoteAddr.Addr] = rc
	}*/
	rc.cnt++
	return rc.rpcClient, nil
}
