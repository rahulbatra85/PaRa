package paxos

import (
	"fmt"
	"net/rpc"
)

//We create this type to register with RPC server
//It's just a wrapper around the PaxosNode type
//All RPC's are forwarded to the PaxosNode object
type PaxosRPCServer struct {
	node *PaxosNode
}

func (s *PaxosRPCServer) startPaxosRPCServer() {
	s.node.DBG("PaxosRPCServer Started")
	for {
		conn, err := s.node.listener.Accept()
		if err != nil {
			fmt.Printf("(%v) PaxosRPCServer Accept Error: %v\n", s.node.Id, err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

//JoinWrapper
func (s *PaxosRPCServer) JoinWrapper(req *JoinRequest, reply *JoinReply) error {
	err := s.node.Join(req)
	reply.Success = err == nil
	return err
}

//StartWrapper
func (s *PaxosRPCServer) StartWrapper(req *StartRequest, reply *StartReply) error {
	err := s.node.Start(req)
	reply.Success = err == nil
	return err
}

//GetStateWrapper
func (s *PaxosRPCServer) GetStateWrapper(req *GetStateRequest, reply *GetStateReply) error {
	err, state := s.node.GetState(req)
	reply.Success = err == nil
	reply.State = state
	return err
}
