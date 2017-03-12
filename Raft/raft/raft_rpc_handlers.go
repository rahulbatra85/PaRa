package raft

import (
	"fmt"
)

//JoinRPC Handler
func (r *RaftNode) Join(request *JoinRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	//Received a Join Request, so append to othersAddr list
	if len(r.othersAddr) == r.config.ClusterSize {
		return fmt.Errorf("Node tried to join after all node have already joined")
	} else {
		r.othersAddr = append(r.othersAddr, request.FromAddr)
	}

	return nil
}

//StartRPC Handler
func (r *RaftNode) Start(request *StartRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	//Set OthersAddr list
	r.INF("Received START")
	r.othersAddr = make([]NodeAddr, len(request.OtherNodes))
	for i, node := range request.OtherNodes {
		r.othersAddr[i].Id = node.Id
		r.othersAddr[i].Addr = node.Addr
		r.INF("OtherNode[%d]=[%v] %v", i, node.Id, node.Addr)
	}

	if r.nodeMgrAddr.Id != "" && r.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&r.nodeMgrAddr, &r.localAddr)
	}

	//Start Server
	go r.run()

	return nil
}

//GetState
func (r *RaftNode) GetState(request *GetStateRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return nil
}
