package paxos

type Acceptor struct {
	Bnum     BallotNum
	Accepted map[int]Pvalue

	P1aCh chan P1aRequest
	P2aCh chan P2aRequest
}

func MakeAcceptor() *Acceptor {
	var a = Acceptor{}

	a.Accepted = make(map[int]Pvalue)
	a.P1aCh = make(chan P1aRequest)
	a.P2aCh = make(chan P2aRequest)

	return &a
}

func (p *PaxosNode) run_acceptor(a *Acceptor) {
	p.INF("Acceptor Started")
	for {
		select {
		case msg := <-a.P1aCh:
			p.DBG("ACC: P1aMsg=%v", msg)
			if CompareBallotNum(msg.Bnum, a.Bnum) == 1 {
				a.Bnum = msg.Bnum
				p.DBG("ACC: Updated Bnum=%v", a.Bnum)
			}
			//Send RPC to scout
			req := P1bRequest{ScoutId: msg.ScoutId, Acceptor: p.localAddr, Bnum: a.Bnum, Rval: a.Accepted}
			go p.P1bRPC(&msg.Leader, req)

		case msg := <-a.P2aCh:
			p.DBG("ACC: P2aMsg=%v", msg)
			if CompareBallotNum(msg.Pval.B, a.Bnum) >= 0 {
				a.Bnum = msg.Pval.B
			}

			//Add or Update Pval for the slot in the Accepted Pval set
			//We implemented the optimization in Sec 4.1 of the paper
			//Renesse, et. all where acceptor only maintains the
			//highest pvalue for each slot
			sPval, ok := a.Accepted[msg.Pval.S]
			if !ok {
				a.Accepted[msg.Pval.S] = msg.Pval
			} else {
				if CompareBallotNum(msg.Pval.B, sPval.B) == 1 {
					a.Accepted[msg.Pval.S] = msg.Pval
				}
			}

			//Send Response to commander
			req := P2bRequest{CommanderId: msg.CommanderId, Acceptor: p.localAddr, Bnum: a.Bnum}
			go p.P2bRPC(&msg.Leader, req)
		}
	}
}
