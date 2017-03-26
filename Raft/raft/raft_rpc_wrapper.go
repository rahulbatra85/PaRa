package raft

import (
	"fmt"
	//"net/rpc"
	"golang.org/x/net/context"
)

//We create this type to register with RPC server
//It's just a wrapper around the RaftNode type
//All RPC's are forwarded to the RaftNode object
type RaftRPCWrapper struct {
	node *RaftNode
}

func (s *RaftRPCWrapper) startRaftRPCWrapper() {
	s.node.DBG("RaftRPCWrapper Started")
	if err := s.node.GRPCServer.Serve(s.node.listener); err != nil {
		fmt.Errorf("failed to serve: %v", err)
		return
	}
}

//JoinRPC
func (s *RaftRPCWrapper) JoinRPC(ctx context.Context, req *JoinRequest) (*JoinReply, error) {
	var reply JoinReply
	err := s.node.Join(req)
	reply.Success = err == nil
	return &reply, err
}

//StartRPC
func (s *RaftRPCWrapper) StartRPC(ctx context.Context, req *StartRequest) (*StartReply, error) {
	var reply StartReply
	err := s.node.Start(req)
	reply.Success = err == nil
	return &reply, err
}

//RequestVoteWrapper
func (s *RaftRPCWrapper) RequestVoteRPC(ctx context.Context, req *RequestVoteArgs) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if s.node.netConfig.GetNetworkConfig(s.node.localAddr, *req.FromNode) == false {
		s.node.INF("RequestVote RCV NOT Allowed")
		return nil, fmt.Errorf("Not allowed")
	}
	s.node.INF("RequestVote Wrapper")
	err := s.node.RequestVote(req, &reply)
	return &reply, err
}

//AppendEntriesWrapper
func (s *RaftRPCWrapper) AppendEntriesRPC(ctx context.Context, req *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if s.node.netConfig.GetNetworkConfig(s.node.localAddr, *req.FromNode) == false {
		s.node.INF("AppEntries RCV NOT Allowed")
		return nil, fmt.Errorf("Not allowed")
	}
	s.node.INF("AppendEntries Wrapper")
	err := s.node.AppendEntries(req, &reply)
	return &reply, err
}

//ClientRegisterRequestWrapper
func (s *RaftRPCWrapper) ClientRegisterRequestRPC(ctx context.Context, req *ClientRegisterArgs) (*ClientRegisterReply, error) {
	var reply ClientRegisterReply
	var err error
	err = s.node.ClientRegister(req, &reply)
	return &reply, err
}

//ClientRequestWrapper
func (s *RaftRPCWrapper) ClientRequestRPC(ctx context.Context, req *ClientRequestArgs) (*ClientReply, error) {
	var reply ClientReply
	var err error
	err = s.node.ClientRequest(req, &reply)
	return &reply, err
}

//GetTermWrapper
func (s *RaftRPCWrapper) GetTermRPC(ctx context.Context, req *GetTermRequest) (*GetTermReply, error) {
	var reply GetTermReply
	err := s.node.GetTerm(req, &reply)
	reply.Success = err == nil
	return &reply, err
}

//GetStateWrapper
func (s *RaftRPCWrapper) GetStateRPC(ctx context.Context, req *GetStateRequest) (*GetStateReply, error) {
	var reply GetStateReply
	err := s.node.GetState(req, &reply)
	reply.Success = err == nil
	return &reply, err
}

//EnableNodeWrapper
func (s *RaftRPCWrapper) EnableNodeRPC(ctx context.Context, req *EnableNodeRequest) (*EnableNodeReply, error) {
	var reply EnableNodeReply
	err := s.node.EnableNode(req, &reply)
	reply.Success = err == nil
	return &reply, err
}

//DisableNodeWrapper
func (s *RaftRPCWrapper) DisableNodeRPC(ctx context.Context, req *DisableNodeRequest) (*DisableNodeReply, error) {
	var reply DisableNodeReply
	err := s.node.DisableNode(req, &reply)
	reply.Success = err == nil
	return &reply, err
}

//SetNodetoNodeWrapper
func (s *RaftRPCWrapper) SetNodetoNodeRPC(ctx context.Context, req *SetNodetoNodeRequest) (*SetNodetoNodeReply, error) {
	var reply SetNodetoNodeReply
	err := s.node.SetNodetoNode(req, &reply)
	reply.Success = err == nil
	return &reply, err
}
