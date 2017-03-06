package paxos

import (
	"fmt"
	"sync"
)

type scoutState int

type Scout struct {
	Id        string
	Acceptors []NodeAddr
	Bnum      BallotNum
	Pvals     []Pvalue
	WaitFor   map[NodeAddr]bool
	P1bCh     chan P1bRequest
}

func CreateScout(acceptors []NodeAddr, b BallotNum, id string) *Scout {
	var s Scout
	s.Id = id
	s.Acceptors = append(s.Acceptors, acceptors...)
	s.Bnum = b

	for a := range acceptors {
		s.WaitFor[a] = true
	}

	//Create a buffered channel, so no RPC ever blocks
	s.P1bCh = make(chan P1bRequest, len(acceptors))

	return &s
}

func (p *PaxosNode) run_scout(s *Scout) {

	//send to all acceptors
	req := P1aRequest{ScoutId: s.Id, Leader: p.localAddr, Bnum: s.Bnum}
	for n := range s.Acceptors {
		go func(n *NodeAddr, r P1aRequest) {
			p.P1aRPC(n, req)
		}(n, req)
	}

	var pvals []Pvalue
	done := false
	for !done {
		select {
		case msg := <-s.P1bCh:
			if msg.B == s.Bnum {
				pvals = append(pvals, msg.R)
				delete(s.WaitFor, msg.NodeAddr)
				if len(s.WaitFor) < (len(s.Acceptors) / 2) {
					m := AdoptedMsg{B: s.Bnum, Pvals: pvals}
					l.AdoptCh <- m
					done := true
				}
			} else {
				m := PreemptedMsg{Bp: s.Bnum}
				l.PreemptCh <- m
				done := true
			}
		}
	}

	//Delete this scout
	l.muScouts.Lock()
	delete(l.Scouts, s.Id)
	l.muScouts.Unlock()
}
