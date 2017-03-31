package paxos

type scoutState int

type Scout struct {
	Id        string
	Acceptors []NodeAddr
	Bnum      BallotNum
	WaitFor   map[NodeAddr]bool
	P1bCh     chan P1bRequest
	L         *Leader
}

func MakeScout(acceptors []NodeAddr, b BallotNum, id string, l *Leader) *Scout {
	var s Scout
	s.Id = id
	s.Acceptors = append(s.Acceptors, acceptors...)
	s.Bnum = b

	for _, a := range acceptors {
		s.WaitFor[a] = true
	}

	//Create a buffered channel, so no RPC ever blocks
	s.P1bCh = make(chan P1bRequest, len(acceptors))
	s.L = l

	return &s
}

func (p *PaxosNode) run_scout(s *Scout) {
	p.DBG("run_scout Enter")
	//send to all acceptors
	req := P1aRequest{ScoutId: s.Id, Leader: p.localAddr, Bnum: s.Bnum}
	for _, n := range s.Acceptors {
		p.P1aRPC(&n, req)
	}

	spvals := make(map[int]Pvalue)
	done := false
	for !done {
		select {
		case msg := <-s.P1bCh:
			if msg.Bnum == s.Bnum {
				for slot, pval := range msg.Rval {
					sPval, ok := spvals[slot]
					if !ok {
						spvals[slot] = pval
					} else {
						if CompareBallotNum(pval.B, sPval.B) == 1 {
							spvals[slot] = pval
						}
					}
				}
				delete(s.WaitFor, msg.Acceptor)
				if len(s.WaitFor) < (len(s.Acceptors) / 2) {
					m := AdoptedMsg{B: s.Bnum, Pvals: spvals}
					s.L.AdoptCh <- m
					done = true
				}
			} else {
				m := PreemptedMsg{Bp: s.Bnum}
				s.L.PreemptCh <- m
				done = true
			}
		}
	}

	//Delete this scout
	s.L.MuScouts.Lock()
	delete(s.L.Scouts, s.Id)
	s.L.MuScouts.Unlock()

	p.DBG("run_scout Exit")
}
