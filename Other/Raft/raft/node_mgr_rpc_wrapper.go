package raft

import (
	"net/rpc"
)

type NodeManagerRPCServer struct {
	node *NodeManager
}

func (s *NodeManagerRPCServer) startNodeManagerRPCServer() {
	s.node.DBG("NodeManagerRPCServer Started")
	for {
		conn, err := s.node.listener.Accept()
		if err != nil {
			s.node.ERR("MgrRPCServer Accept Error: %v\n", err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}

//ReadyNotificationWrapper
func (s *NodeManagerRPCServer) ReadyNotificationWrapper(req *ReadyNotificationRequest, reply *ReadyNotificationReply) error {
	err := s.node.ReadyNotification(req)
	reply.Success = err == nil
	return err
}
