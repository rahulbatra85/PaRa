package paxos

import (
	"fmt"
)

type ReplicaRequest struct {
	c Cmd
}

type ReplicaDecision struct {
	s int
	c Cmd
}

type Replica struct {
	State     interface{}
	SlotIn    int
	SlotOut   int
	Requests  []Cmd
	Proposals []Cmd
	Decisions []Cmd
	Leaders   []int

	ReqCh chan ReplicaRequest
	DecCh chan ReplicaDecision
}

func (r *Replica) CreateReplica(leaders []int, state interface{}) {
	r.leaders = leaders
	r.state = state
	r.slotIn = 1
	r.slotOut = 1

	go run_replica()
}

func (r *Replica) propose() {
}

func (r *Replica) perform() {
}

func (r *Replica) run_replica() {
	for {
		select {
		case msg := <-r.ReqCh:

		case msg := <-r.DecCh:

		}
	}
}
