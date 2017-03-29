package paxos

import (
	"sync"
)

type Leader struct {
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
	Pvals []Pvalue
}

type PreemptedMsg struct {
	Bp BallotNum
}

func MakeLeader(acceptorsReplicas []NodeAddr, lid NodeAddr) *Leader {
	var l Leader

	l.AcceptorsReplicas = append(l.AcceptorsReplicas, acceptorsReplicas...)
	l.Bnum.Id = 0
	l.Bnum.Lid = lid
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
	for {
		select {
		case msg := <-l.ProposeCh:
			if _, ok := l.Proposals[msg.Slot]; !ok {
				l.Proposals[msg.Slot] = msg.Cmd
				if active == true {
					cid := StringBallotSlot(l.Bnum, msg.Slot)
					c := CreateCommander(p.othersAddr, PValue{B: l.Bnum, S: msg.Slot, C: msg.Cmd})
					l.MuCommanders.Lock()
					l.Commanders[cid] = c
					l.MuCommanders.Unlock()
					go p.run_commander(c)
				}
			}
		case msg := <-l.AdoptCh:
			//\todo. Update l.proposals
			for s, v := range l.Proposals {
				cid := StringBallotSlot(l.Bnum, s)
				c := CreateCommander(p.othersAddr, PValue{B: l.Bnum, S: s, C: v}, cid)
				l.MuCommanders.Lock()
				l.Commanders[cid] = c
				l.MuCommanders.Unlock()
				go p.run_commander(c)
			}
		case msg := <-l.PreemptCh:
			if CompareBallotNum(msg.Bp, l.Bnum) == 1 {
				active = false
				l.Bnum.Id = l.Bnum.Id + 1
				sid := StringBallot(l.Bnum)
				s := CreateScout(p.othersAddr, l.b, sid)
				l.MuScouts.Lock()
				l.Scouts[sid] = s
				l.MuScouts.Unlock()
				go p.run_scout(s)
			}
		}
	}
}
