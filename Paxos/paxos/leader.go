package paxos

import (
	"sync"
	"time"
)

type Leader struct {
	LocalAddr         []NodeAddr
	AcceptorsReplicas []NodeAddr
	Bnum              BallotNum
	Active            bool
	Proposals         map[int]Command
	ProposeCh         chan ProposeRequest
	AdoptCh           chan AdoptedMsg
	PreemptCh         chan PreemptedMsg
	Scouts            map[string]*Scout
	Commanders        map[string]*Commander
	MuScouts          sync.RWMutex
	MuCommanders      sync.RWMutex
}

type AdoptedMsg struct {
	B     BallotNum
	Pvals map[int]Pvalue
}

type PreemptedMsg struct {
	Bp BallotNum
}

func MakeLeader(acceptorsReplicas []NodeAddr, lid NodeAddr) *Leader {
	var l Leader

	l.AcceptorsReplicas = append(l.AcceptorsReplicas, acceptorsReplicas...)
	l.Bnum.Id = 0
	l.Bnum.Lid = lid.Id
	l.Active = false
	l.Proposals = make(map[int]Command)
	l.ProposeCh = make(chan ProposeRequest)
	l.AdoptCh = make(chan AdoptedMsg)
	l.PreemptCh = make(chan PreemptedMsg)
	l.Scouts = make(map[string]*Scout)
	l.Commanders = make(map[string]*Commander)

	return &l
}

func (p *PaxosNode) run_leader(l *Leader) {
	p.INF("Leader Started")

	//Start First Scout
	time.Sleep(100 * time.Millisecond)

	sid := StringBallot(l.Bnum)
	s := MakeScout(p.othersAddr, l.Bnum, sid, l)
	l.MuScouts.Lock()
	l.Scouts[sid] = s
	l.MuScouts.Unlock()
	go p.run_scout(s)

	for {
		select {
		case msg := <-l.ProposeCh:
			p.DBG("LEADER:  Propose=%d,%v", msg.Slot, msg.Cmd)
			if _, ok := l.Proposals[msg.Slot]; !ok {
				l.Proposals[msg.Slot] = msg.Cmd
				p.INF("LEADER:  Proposal Added")
				if l.Active == true {
					p.INF("LEADER:  Starting Cmder for Proposal")
					cid := StringBallotSlot(l.Bnum, msg.Slot)
					c := MakeCommander(p.othersAddr, Pvalue{B: l.Bnum, S: msg.Slot, C: msg.Cmd}, cid, l)
					l.MuCommanders.Lock()
					l.Commanders[cid] = c
					l.MuCommanders.Unlock()

					go p.run_commander(c)
				}
			}
		case msg := <-l.AdoptCh:
			p.DBG("LEADER:  AdoptMsg=%v,%v", msg.B, msg.Pvals)
			if CompareBallotNum(msg.B, l.Bnum) == 0 {
				p.DBG("LEADER:  Ballot Matches. Updating Proposals and Starting Cmders")
				for slot, pval := range msg.Pvals {
					l.Proposals[slot] = pval.C
				}
				for slot, cmd := range l.Proposals {
					cmderId := StringBallotSlot(l.Bnum, slot)
					cmder := MakeCommander(p.othersAddr, Pvalue{B: l.Bnum, S: slot, C: cmd}, cmderId, l)
					l.MuCommanders.Lock()
					l.Commanders[cmderId] = cmder
					l.MuCommanders.Unlock()
					go p.run_commander(cmder)
				}
				l.Active = true
			}
		case msg := <-l.PreemptCh:
			p.DBG("LEADER:  PremptMsg=%v", msg.Bp)
			if CompareBallotNum(msg.Bp, l.Bnum) == 1 {
				p.DBG("LEADER:  Ballot Bigger. Starting new scout")
				l.Active = false
				l.Bnum.Id = l.Bnum.Id + 1
				sid := StringBallot(l.Bnum)
				s := MakeScout(p.othersAddr, l.Bnum, sid, l)
				l.MuScouts.Lock()
				l.Scouts[sid] = s
				l.MuScouts.Unlock()
				go p.run_scout(s)
			}
		}
	}
}
