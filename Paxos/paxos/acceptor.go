package paxos

type Acceptor struct {
	Bnum     BallotNum
	Accepted Pvalue

	P1aCh chan P1aRequest
	P2aCh chan P2aRequest
}

func MakeAcceptor() *Acceptor {
	var a = Acceptor{}

	a.P1aCh = make(chan P1aRequest)
	a.P2aCh = make(chan P2aRequest)

	return &a
}

func (p *PaxosNode) run_acceptor(a *Acceptor) {
	for {
		select {
		case msg := <-a.P1aCh:
			if msg.B > a.Bnum {
				a.Bnum = msg.B
			}
			//Send RPC to scout
			req := P1bRequest{ScoutId: msg.ScoutId, Acceptor: p.localAddr, Bnum: a.Bnum, Rval: a.Accepted}
			p.P1bRPC(msg.Leader, req)
		case msg := <-a.P2aCh:
			if msg.B == a.Bnum {
				a.Accepted = msg.Pval
			}
			//Send RPC to commander
			req := P2bRequest{CommanderId: msg.CommanderId, Acceptor: p.localAddr, Bnum: a.Bnum}
			p.P2bRPC(msg.Leader, req)
		}
	}
}
