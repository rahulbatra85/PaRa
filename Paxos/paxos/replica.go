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
	p.DBG("REPLICA: Propose Enter")
	for len(p.r.Requests) > 0 {
		if _, ok := p.r.Decisions[p.r.SlotIn]; !ok {
			cmd := p.r.Requests[0]
			p.r.Proposals[p.r.SlotIn] = cmd
			p.r.Requests = p.r.Requests[1:len(p.r.Requests)]
			//Send propose to all leaders
			p.DBG("REPLICA: Moved request %v to proposal slot %d", cmd, p.r.SlotIn)
			req := ProposeRequest{Slot: p.r.SlotIn, Cmd: cmd}
			for _, l := range r.Leaders {
				if l != p.localAddr {
					go func(n NodeAddr, req ProposeRequest) {
						p.ProposeRPC(&n, req)
					}(l, req)
				}
			}
			go func() {
				p.l.ProposeCh <- req
			}()
		}
		p.r.SlotIn++
	}
	p.DBG("REPLICA: Propose Exit")
}

func (p *PaxosNode) perform(r *Replica, c Command) {
	p.DBG("REPLICA: Perform Cmd,%v", c)
	for s := 0; s < p.r.SlotOut; s++ {
		if IsSameCmd(r.Decisions[s], c) {
			p.DBG("REPLICA: Duplicate Command")
			r.SlotOut++
			return
		}
	}

	reply := ClientReply{}
	reply.SeqNum = c.SeqNum
	reply.Value, reply.Code = p.app.ApplyOperation(c)
	id := fmt.Sprintf("%v_%v", c.ClientId, c.SeqNum)
	go func(ch chan ClientReply, rep ClientReply) {
		ch <- reply
		p.DBG("REPLICA: Command Performed")
	}(r.ClientRequestMsgMap[id].reply, reply)

	delete(r.ClientRequestMsgMap, id)
	r.ClientReplyMap[id] = reply
	r.SlotOut++
}

func (p *PaxosNode) run_replica(r *Replica) {
	p.INF("Replica Started")
	for {
		select {
		//Request From Client
		case msg := <-r.ReqMsgCh:
			p.DBG("REPLICA: RequestMsg=%v", msg.args)
			//Check if this is a duplicate command
			id := fmt.Sprintf("%v_%v", msg.args.Cmd.ClientId, msg.args.Cmd.SeqNum)
			if reply, ok := r.ClientReplyMap[id]; ok {
				if reply.SeqNum == msg.args.Cmd.SeqNum {
					p.DBG("REPLICA: RequestReplayed")
					msg.reply <- reply
				}
			} else {
				r.Requests = append(r.Requests, msg.args.Cmd)
				r.ClientRequestMsgMap[id] = msg
				p.DBG("REPLICA: Request Added")
			}

		//Decision
		case msg := <-r.DecCh:
			p.DBG("REPLICA: Decision=%d,%v", msg.Slot, msg.Cmd)
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
