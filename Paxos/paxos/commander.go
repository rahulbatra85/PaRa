package paxos

type Commander struct {
	Id      string
	Nodes   []NodeAddr
	Pval    Pvalue
	WaitFor map[NodeAddr]bool
	P2bCh   chan P2bRequest
	L       *Leader
}

func MakeCommander(nodes []NodeAddr, pval Pvalue, id string, leader *Leader) *Commander {
	var c Commander
	c.Id = id
	c.Nodes = append(c.Nodes, nodes...)
	c.Pval = pval

	c.WaitFor = make(map[NodeAddr]bool)
	for _, n := range nodes {
		c.WaitFor[n] = true
	}

	c.P2bCh = make(chan P2bRequest, len(nodes))
	c.L = leader

	return &c
}

func (p *PaxosNode) run_commander(c *Commander) {
	p.INF("run_commander Enter")
	//send to all acceptors
	req := P2aRequest{CommanderId: c.Id, Leader: p.localAddr, Pval: c.Pval}
	for _, n := range c.Nodes {
		go func(n *NodeAddr, r P2aRequest) {
			p.P2aRPC(n, req)
		}(&n, req)
	}

	done := false
	for !done {
		select {
		case msg := <-c.P2bCh:
			if CompareBallotNum(msg.Bnum, c.Pval.B) == 0 {
				delete(c.WaitFor, msg.Acceptor)
				if len(c.WaitFor) < (len(c.Nodes) / 2) {
					//Send decision to Replicas
					req := DecisionRequest{Slot: c.Pval.S, Cmd: c.Pval.C}
					for _, n := range c.Nodes {
						p.DecisionRPC(&n, req)
					}
					done = true
				}
			} else {
				//Send preempted to leader
				m := PreemptedMsg{Bp: msg.Bnum}
				c.L.PreemptCh <- m
				done = true
			}
		}
	}

	//Delete this commander
	c.L.MuCommanders.Lock()
	delete(c.L.Commanders, c.Id)
	c.L.MuCommanders.Unlock()
	p.INF("run_commander Exit")
}
