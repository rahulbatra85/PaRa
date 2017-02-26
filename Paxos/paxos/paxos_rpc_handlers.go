package paxos

import (
	"fmt"
)

//JoinRPC Handler
func (p *PaxosNode) Join(request *JoinRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	//Received a Join Request, so append to othersAddr list
	if len(p.othersAddr) == p.config.ClusterSize {
		return fmt.Errorf("Node tried to join after all node have already joined")
	} else {
		p.othersAddr = append(p.othersAddr, request.FromAddr)
	}

	return nil
}

//StartRPC Handler
func (p *PaxosNode) Start(request *StartRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	//Set OthersAddr list
	p.INF("Received START")
	p.othersAddr = make([]NodeAddr, len(request.OtherNodes))
	for i, node := range request.OtherNodes {
		p.othersAddr[i].Id = node.Id
		p.othersAddr[i].Addr = node.Addr
		p.INF("OtherNode[%d]=[%v] %v", i, node.Id, node.Addr)
	}

	if p.nodeMgrAddr.Id != "" && p.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&p.nodeMgrAddr, &p.localAddr)
	}

	//Start Server
	go p.run()

	return nil
}

//GetState
func (p *PaxosNode) GetState(request *GetStateRequest) (error, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return nil, p.ballotNum
}
