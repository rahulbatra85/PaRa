package raft

import (
	"crypto/sha1"
	"math/big"
)

/*
type RaftState int

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)


type NodeAddr struct {
	Id   string
	Addr string
}

type Command struct {
	ClientNodeAddr NodeAddr
	Cid            int
	SeqNum         int
	Op             Operation
}

//\todo make it generic
type Operation struct {
	OpType string
	Key    string
	Data   string
}
*/
type Application interface {
	ApplyOperation(op Operation) string
}

func HashAddr(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	ha := h.Sum(nil)
	id := big.Int{}
	id.SetBytes(ha[:length])
	return id.String()
}
