package paxos

func (p *PaxosNode) run() {
	p.INF("PaxosServer Run Enter")

	//Create Replica
	p.r = MakeReplica(p.othersAddr, p.app)
	//Create Leader
	p.l = MakeLeader(p.othersAddr, p.localAddr)
	//Create Acceptor
	p.a = MakeAcceptor()

	//Start Replica
	go p.run_replica(p.r)
	//Start Leader
	go p.run_leader(p.l)
	//Start Acceptor
	go p.run_acceptor(p.a)
	p.INF("PaxosServer Run Exit")
}
