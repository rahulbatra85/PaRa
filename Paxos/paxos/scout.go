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

	s.WaitFor = make(map[NodeAddr]bool)
	for _, a := range acceptors {
		s.WaitFor[a] = true
	}

	//Create a buffered channel, so no RPC ever blocks
	s.P1bCh = make(chan P1bRequest, len(acceptors))
	s.L = l

	return &s
}

func (p *PaxosNode) run_scout(s *Scout) {
	p.INF("SCOUT[%s]: Started", s.Id)
	//send to all acceptors
	req := P1aRequest{ScoutId: s.Id, Leader: p.localAddr, Bnum: s.Bnum}
	for _, n := range s.Acceptors {
		if n != p.localAddr {
			go p.P1aRPC(&n, req)
		}
	}
	go func() {
		p.a.P1aCh <- req
	}()

	spvals := make(map[int]Pvalue)
	done := false
	for !done {
		select {
		case msg := <-s.P1bCh:
			p.DBG("SCOUT[%s]: P1bMsg=%v", s.Id, msg)
			if msg.Bnum == s.Bnum {
				p.INF("SCOUT[%s]: BallotMatched", s.Id)
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
				if len(s.WaitFor) < ((len(s.Acceptors) + 1) / 2) {
					p.DBG("SCOUT[%s]: Adopted Bnum=%v", s.Id, s.Bnum)
					m := AdoptedMsg{B: s.Bnum, Pvals: spvals}
					s.L.AdoptCh <- m
					done = true
				}
			} else {
				p.DBG("SCOUT[%s]: Prempted", s.Id)
				m := PreemptedMsg{Bp: msg.Bnum}
				s.L.PreemptCh <- m
				done = true
			}
		}
	}

	//Delete this scout
	s.L.MuScouts.Lock()
	delete(s.L.Scouts, s.Id)
	s.L.MuScouts.Unlock()

	p.DBG("SCOUT[%s]: Exit", s.Id)
}
