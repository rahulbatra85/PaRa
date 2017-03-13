package raft

import (
	"fmt"
	"net/rpc"
)

//We create this type to register with RPC server
//It's just a wrapper around the RaftNode type
//All RPC's are forwarded to the RaftNode object
type RaftRPCServer struct {
	node *RaftNode
}

func (s *RaftRPCServer) startRaftRPCServer() {
	s.node.DBG("RaftRPCServer Started")
	for {
		conn, err := s.node.listener.Accept()
		if err != nil {
			fmt.Printf("(%v) RaftRPCServer Accept Error: %v\n", s.node.Id, err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

//JoinWrapper
func (s *RaftRPCServer) JoinWrapper(req *JoinRequest, reply *JoinReply) error {
	err := s.node.Join(req)
	reply.Success = err == nil
	return err
}

//StartWrapper
func (s *RaftRPCServer) StartWrapper(req *StartRequest, reply *StartReply) error {
	err := s.node.Start(req)
	reply.Success = err == nil
	return err
}

//RequestVoteWrapper
func (s *RaftRPCServer) RequestVoteWrapper(req *RequestVoteArgs, reply *RequestVoteReply) error {
	err := s.node.RequestVote(req, reply)
	return err
}

//AppendEntriesWrapper
func (s *RaftRPCServer) AppendEntriesWrapper(req *AppendEntriesArgs, reply *AppendEntriesReply) error {
	err := s.node.AppendEntries(req, reply)
	return err
}

//GetStateWrapper
func (s *RaftRPCServer) GetStateWrapper(req *GetStateRequest, reply *GetStateReply) error {
	err := s.node.GetState(req)
	reply.Success = err == nil
	return err
}
