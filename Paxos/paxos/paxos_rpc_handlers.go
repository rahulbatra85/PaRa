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

func (p *PaxosNode) Propose(request *ProposeRequest) error {
	//forward to leader
	p.l.ProposeCh <- request
	return nil
}
func (p *PaxosNode) Decision(request *DecisionRequest) error {
	//forward to replica
	p.r.DecCh <- request
	return nil
}

func (p *PaxosNode) P1a(request *P1aRequest) error {
	//forward to acceptor
	p.a.P1aCh < request
	return nil
}

func (p *PaxosNode) P2a(request *P2aRequest) error {
	//forward to acceptor
	p.a.P2aCh <- request
	return nil
}

func (p *PaxosNode) P1b(request *P1bRequest) error {
	//forward to scout
	//See if Scout is still active.Otherwise, ignore
	p.l.MuScouts.RLock()
	if s, ok := p.l.Scouts[request.Scout]; ok {
		s.P1bCh <- request
	}
	p.l.MuScouts.Unlock()
}

func (p *PaxosNode) P2b(request *P2bRequest) error {
	//forward to commander
	//See if Commander is still active. Otherwise, ignore
	p.l.MuCommanders.RLock()
	if c, ok := p.l.Commanders[request.CommanderId]; ok {
		c.P2bCh <- request
	}
	p.l.MuScouts.Unlock()
}

//GetState
func (p *PaxosNode) GetState(request *GetStateRequest) (error, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return nil, p.ballotNum
}
