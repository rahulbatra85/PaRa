package raft

import (
	//"fmt"
	"net/rpc"
	//"golang.org/x/net/context"
)

//We create this type to register with RPC server
//It's just a wrapper around the RaftNode type
//All RPC's are forwarded to the RaftNode object
type RaftRPCWrapper struct {
	node *RaftNode
}

func (s *RaftRPCWrapper) startRaftRPCWrapper() {
	s.node.INF("Starting RPC Server")
	for {

		conn, err := s.node.listener.Accept()
		if err != nil {
			s.node.ERR("RaftRPCServer ERROR: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

//JoinRPC
func (s *RaftRPCWrapper) JoinRPC(req *JoinRequest, reply *JoinReply) error {
	s.node.DBG("JoinRPC Enter")
	err := s.node.Join(req)
	reply.Success = err == nil
	s.node.DBG("JoinRPC Exit")
	return err
}

//StartRPC
func (s *RaftRPCWrapper) StartRPC(req *StartRequest, reply *StartReply) error {
	s.node.DBG("StartRPC Enter")
	err := s.node.Start(req)
	reply.Success = err == nil
	s.node.DBG("StartRPC Exit")
	return err
}

//RequestVoteWrapper
func (s *RaftRPCWrapper) RequestVoteRPC(req *RequestVoteArgs, reply *RequestVoteReply) error {
	/*
		if s.node.netConfig.GetNetworkConfig(s.node.localAddr, *req.FromNode) == false {
			s.node.INF("RequestVote RCV NOT Allowed")
			return nil, fmt.Errorf("Not allowed")
		}*/
	s.node.DBG("RequestVote Wrapper")
	rep, err := s.node.RequestVote(req)
	*reply = rep
	return err
}

//AppendEntriesWrapper
func (s *RaftRPCWrapper) AppendEntriesRPC(req *AppendEntriesArgs, reply *AppendEntriesReply) error {
	/*
		if s.node.netConfig.GetNetworkConfig(s.node.localAddr, *req.FromNode) == false {
			s.node.INF("AppEntries RCV NOT Allowed")
			return nil, fmt.Errorf("Not allowed")
		}*/
	s.node.DBG("AppendEntries Wrapper")
	rep, err := s.node.AppendEntries(req)
	*reply = rep
	return err
}

//ClientRegisterRequestWrapper
func (s *RaftRPCWrapper) ClientRegisterRequestRPC(req *ClientRegisterArgs, reply *ClientRegisterReply) error {
	rep, err := s.node.ClientRegister(req)
	*reply = rep
	return err
}

//ClientRequestWrapper
func (s *RaftRPCWrapper) ClientRequestRPC(req *ClientRequestArgs, reply *ClientReply) error {
	rep, err := s.node.ClientRequest(req)
	*reply = rep
	return err
}

//GetTermWrapper
func (s *RaftRPCWrapper) GetTermRPC(req *GetTermRequest, reply *GetTermReply) error {
	rep, err := s.node.GetTerm(req)
	*reply = rep
	return err
}

//GetStateWrapper
func (s *RaftRPCWrapper) GetStateRPC(req *GetStateRequest, reply *GetStateReply) error {
	rep, err := s.node.GetState(req)
	*reply = rep
	return err
}

//EnableNodeWrapper
func (s *RaftRPCWrapper) EnableNodeRPC(req *EnableNodeRequest, reply *EnableNodeReply) error {
	rep, err := s.node.EnableNode(req)
	*reply = rep
	return err
}

//DisableNodeWrapper
func (s *RaftRPCWrapper) DisableNodeRPC(req *DisableNodeRequest, reply *DisableNodeReply) error {
	rep, err := s.node.DisableNode(req)
	*reply = rep
	return err
}

//SetNodetoNodeWrapper
func (s *RaftRPCWrapper) SetNodetoNodeRPC(req *SetNodetoNodeRequest, reply *SetNodetoNodeReply) error {
	rep, err := s.node.SetNodetoNode(req)
	*reply = rep
	return err
}
