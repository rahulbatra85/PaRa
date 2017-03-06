package paxos

import (
	"fmt"
)

type Replica struct {
	App       *Application
	SlotIn    int
	SlotOut   int
	Requests  []Cmd
	Proposals map[int]Command
	Decisions map[int]Command
	Leaders   []NodeAddr

	ReqCh chan RRequestMsg
	DecCh chan RReplicaDecision
}

func CreateReplica(leaders []NodeAddr, app *Application) *Replica {
	var r = Replica{}
	r.App = app
	r.SlotIn = 1
	r.SlotOut = 1
	r.Proposals = make(map[int]Command)
	r.Decisions = make(map[int]Command)
	r.Leaders = append(r.Leaders, leaders...)

	r.ReqCh = make(chan RRequestMsg)
	r.DecCh = make(chan RReplicaDecision)

	return &r
}

func (p *PaxosNode) propose(r *Replica) {
	for len(p.r.Requests) > 0 {
		if _, ok := p.r.Decisions[p.r.SlotIn]; !ok {
			proposals[p.r.SlotIn] = p.r.Requests[0]
			p.r.requests = p.r.requests[1:len(p.r.Requests)]
			//Send propose to all leaders
		}
		p.r.SlotIn++
	}
}

func (p *PaxosNode) perform(r *Replica, c Command) {
	//\todo
}

func (p *PaxosNode) run_replica(r *Replica) {

	for {
		select {
		case msg := <-r.ReqCh:
			r.Requests = append(r.Requests, msg.c)

		case msg := <-r.DecCh:
			r.Decisions[msg.s] = msg.c
			cmdDecision, cmdInDecisions := r.Decisions[r.SlotOut]
			for cmdInDecision == true {
				if cmdProposal, cmdInProposals := r.Proposals[r.SlotOut]; cmdInProp {
					delete(r.Proposals, r.SlotOut)
					if cmdProposal.Cid != cmdDecision.Cid || cmdProposal.SeqNum != cmdDecision.SeqNum || cmdProposal.Operation.Name != cmdDecision.Operation.Name {
						r.Requests = append(r.Requests, cmdDecision)
					}
				}

				p.Perform(r)
			}

		}
	}
}
