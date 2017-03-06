package paxos

import ()

func (p *PaxosNode) run() {
	p.INF("PaxosServer Running")

	//Create Replica
	p.r = CreateReplica(p.othersAddr, p.App)
	//Create Leader
	p.l = CreateLeader(p.othersAddr, p.localAddr)
	//Create Acceptor
	p.a = CreateAcceptor()

	//Start Replica
	p.run_replica(p.r)
	//Start Leader
	p.run_leader(p.l)
	//Start Acceptor
	p.run_acceptor(p.a)
}
