//Name: common.go
//Description: Contains structure common to Key-Value Server and Client
//Author: Rahul Batra

package paxos_kv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID int
	ReqID    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int
	ReqID    int
}

type GetReply struct {
	Err   Err
	Value string
}

type ClientApplied struct {
	ReqID int
	Err   Err
	Value string
}
