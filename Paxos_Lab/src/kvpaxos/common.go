package kvpaxos

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
	ReqID    int64
	ClientID string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ReqID    int64
	ClientID string
}

type GetReply struct {
	Err   Err
	Value string
}
