package paxos

import (
	"fmt"
)

type Acceptor struct {
	BallotNum int
	Accepted  []Pvalue

	P1a chan p1aMsg
	P2a chan p2aMsg
}

func (a *Acceptor) CreateAcceptor() {
	a.BallotNum = -1

	go run_acceptor()
}

func (a *Acceptor) run_acceptor() {
	for {
		select {
		case msg := <-a.P1a:
		case msg := <-a.P2a:
		}
	}
}
