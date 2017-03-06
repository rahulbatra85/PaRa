package paxos

import (
	"fmt"
)

type Commander struct {
	Id      string
	Nodes   []NodeAddr
	Pval    Pvalue
	WaitFor map[NodeAddr]bool
	P2bCh   chan P2bRequest
}

func CreateCommander(nodes []NodeAddr, pval Pvalue, id string) *Commander {
	var c Commander
	c.Id = id
	c.Nodes = append(p.a.Nodes, nodes...)
	c.Pval = pval

	c.WaitFor = make(map[NodeAddr]bool)
	for n := range nodes {
		c.WaitFor[n] = true
	}

	c.P2bCh = make(chan P2bRequest, len(nodes))

	return &c
}

func (p *PaxosNode) run_commander(c *Commander) {

	//send to all acceptors
	req = P2aRequest{CommanderId: c.Id, Leader: p.localAddr, Pval: c.Pval}
	for n := range c.Nodes {
		go func(n *NodeAddr, r P2aRequest) {
			p.P2aRPC(n, req)
		}(n, req)
	}

	done := false
	for !done {
		select {
		case msg := <-c.P2bCh:
			if msg.B == c.Pval.B {
				delete(c.WaitFor, msg.Aid)
				if len(c.WaitFor) < (len(c.Nodes) / 2) {
					//Send decision to Replicas
					req := DecisionRequest{Slot: c.Pval.S, Cmd: c.Pval.C}
					for n := range c.Nodes {
						go func(n *NodeAddr, r P2aRequest) {
							p.DecisionRPC(n, req)
						}(n, req)
					}
					done := true
				}
			} else {
				//Send preempted to leader
				m := PreemptedMsg{Bp: s.Bnum}
				l.PreemptCh <- m
				done = true
			}
		}
	}

	//Delete this commander
	l.MuCommanders.Lock()
	delete(l.Commanders, c.Id)
	l.MuCommanders.Unlock()
}
