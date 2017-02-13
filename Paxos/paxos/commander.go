package paxos

import (
	"fmt"
)

type Commander struct {
	Acceptors []Acceptor
	Leaders   Leader
	Replicas  Replica

	P2b chan P2bMsg
}

func (c *Commander) CreateCommander(leader Leader, acceptors []Acceptor, replicas Replica) {
	c.Leader = leader
	c.Acceptors = acceptors

	c.P2b = make(chan P2bMsg)

	go run_commander()
}

func (c *Commander) run_commander() {
	for {
		select {
		case msg := <-c.P2b:
		}
	}

}
