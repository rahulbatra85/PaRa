package paxos

import (
	"fmt"
)

type Replica struct {
	App       *KVApp
	SlotIn    int
	SlotOut   int
	Requests  []Command
	Proposals map[int]Command
	Decisions map[int]Command
	Leaders   []NodeAddr

	ReqMsgCh chan ClientRequestMsg
	DecCh    chan DecisionRequest

	ClientReplyMap      map[string]ClientReply
	ClientRequestMsgMap map[string]ClientRequestMsg
}

func MakeReplica(leaders []NodeAddr, app *KVApp) *Replica {
	var r = Replica{}
	r.App = app
	r.SlotIn = 1
	r.SlotOut = 1
	r.Proposals = make(map[int]Command)
	r.Decisions = make(map[int]Command)
	r.Leaders = append(r.Leaders, leaders...)

	r.ReqMsgCh = make(chan ClientRequestMsg)
	r.DecCh = make(chan DecisionRequest)

	r.ClientReplyMap = make(map[string]ClientReply)
	r.ClientRequestMsgMap = make(map[string]ClientRequestMsg)
	return &r
}

func (p *PaxosNode) propose(r *Replica) {
	for len(p.r.Requests) > 0 {
		if _, ok := p.r.Decisions[p.r.SlotIn]; !ok {
			cmd := p.r.Requests[0]
			p.r.Proposals[p.r.SlotIn] = cmd
			p.r.Requests = p.r.Requests[1:len(p.r.Requests)]
			//Send propose to all leaders
			req := ProposeRequest{Slot: p.r.SlotIn, Cmd: cmd}
			for _, l := range r.Leaders {
				go p.ProposeRPC(&l, req)
			}
		}
		p.r.SlotIn++
	}
}

func (p *PaxosNode) perform(r *Replica, c Command) {
	for s := 0; s < p.r.SlotOut; s++ {
		if IsSameCmd(r.Decisions[s], c) {
			r.SlotOut++
			return
		}
	}

	reply := ClientReply{}
	reply.SeqNum = c.SeqNum
	reply.Value, reply.Code = p.app.ApplyOperation(c)
	id := fmt.Sprintf("%v_%v", c.ClientId, c.SeqNum)
	r.ClientRequestMsgMap[id].reply <- reply
	delete(r.ClientRequestMsgMap, id)
	r.ClientReplyMap[id] = reply

	r.SlotOut++
}

func (p *PaxosNode) run_replica(r *Replica) {
	for {
		select {
		//Request From Client
		case msg := <-r.ReqMsgCh:
			//Check if this is a duplicate command
			id := fmt.Sprintf("%v_%v", msg.args.Cmd.ClientId, msg.args.Cmd.SeqNum)
			if reply, ok := r.ClientReplyMap[id]; ok {
				if reply.SeqNum == msg.args.Cmd.SeqNum {
					msg.reply <- reply
				}
			} else {
				r.Requests = append(r.Requests, msg.args.Cmd)
				r.ClientRequestMsgMap[id] = msg
			}

		//Decision
		case msg := <-r.DecCh:
			r.Decisions[msg.Slot] = msg.Cmd
			cmdDecision, cmdInDecisions := r.Decisions[r.SlotOut]
			for cmdInDecisions == true {
				//Update Proposals
				if cmdProposal, cmdInProposals := r.Proposals[r.SlotOut]; cmdInProposals {
					delete(r.Proposals, r.SlotOut)
					if IsSameCmd(cmdProposal, cmdDecision) != true {
						r.Requests = append(r.Requests, cmdProposal)
					}
				}

				p.perform(r, cmdDecision)
				cmdDecision, cmdInDecisions = r.Decisions[r.SlotOut]
			} //end for

		} //end select
		p.propose(r)
	} //end for
}
