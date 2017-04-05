package paxos

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

type ClientReplyCode int

const (
	ClientReplyCode_REQUEST_FAILED ClientReplyCode = iota
	ClientReplyCode_REQUEST_SUCCESSFUL
	ClientReplyCode_INVALID_KEY
	ClientReplyCode_INVALID_COMMAND
)

type OpType int

const (
	OpType_CLIENT_REGISTRATION OpType = iota
	OpType_GET                        = 1
	OpType_PUT                        = 2
)

type NodeAddr struct {
	Id   string
	Addr string
}

type Command struct {
	ClientId int
	SeqNum   int
	Op       Operation
}

type Operation struct {
	Type  OpType
	Key   string
	Value string
}

func IsSameCmd(c1 Command, c2 Command) bool {
	if c1.ClientId == c2.ClientId && c1.SeqNum == c2.SeqNum {
		return true
	} else {
		return false
	}
}

//Ballot Numbers are lexicographically ordered
//pair of an integer and leader id. Leader id
//must be totally ordered
//Note: NULL Ballot Number-(Id:0,Lid:"")
type BallotNum struct {
	Id  int    //Ballot Num Id
	Lid string //Leader Id
}

//Proposal Value is a triple consisting of BallotNum,slot number, and command
type Pvalue struct {
	B BallotNum
	S int
	C Command
}

//Compares BallotNum.
//Returns -1 if b1 < b2, 0 if b1 == b2, or 1 if b1 > b2
func CompareBallotNum(b1 BallotNum, b2 BallotNum) int {
	if b1.Id < b2.Id {
		return -1
	} else if b1.Id > b2.Id {
		return 1
	} else {
		if b1.Lid < b2.Lid {
			return -1
		} else if b1.Lid > b2.Lid {
			return 1
		} else {
			return 0
		}
	}
}

func StringBallot(b BallotNum) string {
	s := fmt.Sprintf("%v_%v", b.Id, b.Lid)
	return s
}

func StringBallotSlot(b BallotNum, slot int) string {
	s := fmt.Sprintf("%v_%v_%v", b.Id, b.Lid, slot)
	return s
}

func HashAddr(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	ha := h.Sum(nil)
	id := big.Int{}
	id.SetBytes(ha[:length])
	return id.String()
}
