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

//Propose Wrapper
func (s *PaxosRPCServer) ProposeWrapper(req *ProposeRequest, reply *ProposeReply) error {
	//\todo add enable check
	err := s.node.Propose(req)
	reply.Success = err == nil
	return err
}

//Decision Wrapper
func (s *PaxosRPCServer) DecisionWrapper(req *DecisionRequest, reply *DecisionReply) error {
	//\todo add enable check
	err := s.node.Decision(req)
	reply.Success = err == nil
	return err
}

//P1a Wrapper
func (s *PaxosRPCServer) P1aWrapper(req *P1aRequest, reply *P1aReply) error {
	//\todo add enable check
	err := s.node.P1a(req)
	reply.Success = err == nil
	return err
}

//P2a Wrapper
func (s *PaxosRPCServer) P2aWrapper(req *P2aRequest, reply *P2aReply) error {
	err := s.node.P2a(req)
	reply.Success = err == nil
	return err
}

//P1b Wrapper
func (s *PaxosRPCServer) P1bWrapper(req *P1bRequest, reply *P1bReply) error {
	err := s.node.P1b(req)
	reply.Success = err == nil
	return err
}

//P2b Wrapper
func (s *PaxosRPCServer) P2bWrapper(req *P2bRequest, reply *P2bReply) error {
	err := s.node.P2b(req)
	reply.Success = err == nil
	return err
}

//
//ClientRegisterWrapper
func (s *PaxosRPCServer) ClientRegisterWrapper(req *ClientRegisterArgs, reply *ClientRegisterReply) error {
	err := s.node.ClientRegisterHdl(req)
	reply.Success = err == nil
	return err
}

//
//ClientRequestWrapper
func (s *PaxosRPCServer) ClientRequestWrapper(req *ClientRequestArgs, reply *ClientReply) error {
	err := s.node.ClientRequestHdl(req)
	reply.Success = err == nil
	return err
}
