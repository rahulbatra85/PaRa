package raft

import (
	"crypto/sha1"
	"math/big"
)

type RaftState int

const (
	RaftState_FOLLOWER RaftState = iota
	RaftState_CANDIDATE
	RaftState_LEADER
)

type ClientReplyCode int

const (
	ClientReplyCode_REQUEST_FAILED ClientReplyCode = iota
	ClientReplyCode_REQUEST_SUCCESSFUL
	ClientReplyCode_RETRY
	ClientReplyCode_NOT_LEADER
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
	ClientId int32
	SeqNum   uint64
	Op       *Operation
}

type Operation struct {
	Type  OpType
	Key   string
	Value string
}

type LogEntry struct {
	Term int32
	Cmd  *Command
}

//Join Request
type JoinRequest struct {
	RemoteNode *NodeAddr
	FromNode   *NodeAddr
}

type JoinReply struct {
	Success bool
}

//Start Request
type StartRequest struct {
	RemoteNode *NodeAddr
	OtherNodes []*NodeAddr
}

type StartReply struct {
	Success bool
}

//RequestVoteRPC
type RequestVoteArgs struct {
	FromNode    *NodeAddr
	Term        int32
	CandidateId string
	LastLogIdx  int32
	LastLogTerm int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

//AppendEntriesRPC
type AppendEntriesArgs struct {
	FromNode     *NodeAddr
	Term         int32
	LeaderId     string
	PrevLogIdx   int32
	PrevLogTerm  int32
	Entries      []*LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

/////////////////////////////////////////
//Node Manager
/////////////////////////////////////////

//GetTerm
type GetTermRequest struct {
	RemoteNode *NodeAddr
}

type GetTermReply struct {
	Term    int32
	Success bool
}

//GetState
type GetStateRequest struct {
	RemoteNode *NodeAddr
}

type GetStateReply struct {
	State   RaftState
	Success bool
}

//Enable Node
type EnableNodeRequest struct {
}

type EnableNodeReply struct {
	Success bool
}

//Disable Node
type DisableNodeRequest struct {
}

type DisableNodeReply struct {
	Success bool
}

//SetNodetoNode
type SetNodetoNodeRequest struct {
	ToNode *NodeAddr
	Enable bool
}

type SetNodetoNodeReply struct {
	Success bool
}

/////////////////////////////////////////
//Client
/////////////////////////////////////////

//Register
type ClientRegisterArgs struct {
	FromNode *NodeAddr
}

type ClientRegisterReply struct {
	Code       ClientReplyCode
	ClientId   int32
	LeaderNode *NodeAddr
}

//Request
type ClientRequestArgs struct {
	Cmd *Command
}

type ClientReply struct {
	Code       ClientReplyCode
	LeaderNode *NodeAddr
	Value      string
	SeqNum     uint64
}

func HashAddr(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	ha := h.Sum(nil)
	id := big.Int{}
	id.SetBytes(ha[:length])
	return id.String()
}
