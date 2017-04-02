package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	ReqId    int
}

type PutAppendReply struct {
	Err         Err
	WrongLeader int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	ReqId    int
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader int
}

type ClientReply struct {
	ReqId       int
	WrongLeader int
	Err         Err
	Value       string
}
