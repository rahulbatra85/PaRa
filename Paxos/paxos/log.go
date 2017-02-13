package paxos

type Cmd struct {
	ClientID int
	CmdID    int
	Cmd      interface{}
}
