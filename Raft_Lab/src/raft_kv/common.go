//Name: raft.go
//Description: Main file for raft client
//Author: Rahul Batra

package raft_kv

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	OldRequest = "OldRequest"
)

type Err string

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int
	ReqId    int
}

type PutAppendReply struct {
	Err         Err
	WrongLeader int
}

type GetArgs struct {
	Key      string
	ClientId int
	ReqId    int
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader int
}

type ClientApply struct {
	ClientId    int
	ReqId       int
	WrongLeader int
	Err         Err
	Value       string
}
