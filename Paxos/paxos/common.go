package paxos

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

type Application interface {
	ApplyOperation(op Operation) string
}

//Ballot Numbers are lexicographically ordered
//pair of an integer and leader id. Leader id
//must be totally ordered
type BallotNum struct {
	Id  int //Ballot Num Id
	Lid int //Leader Id
}

//Proposal Value is a triple consisting of BallotNum,slot number, and command
type Pvalue struct {
	B BallotNum
	S int
	C Command
}

/*
//Messages rcvd by Replica
type RequestMsg struct {
	C Command
}

type DecisionMsg struct {
	S int
	C Command
}

//Messages rcvd by Acceptor
type P1aMsg struct {
	Lid NodeAddr
	B   BallotNum
}

type P2aMsg struct {
	Lid  NodeAddr
	Pval Pvalue
}

//Message rcvd by Scout
type P1bMsg struct {
	Aid NodeAddr
	B   BallotNum
	R   Pvalue
}

//Message rcvd by Commander
type P2bMsg struct {
	Aid NodeAddr
	B   BallotNum
}

*/

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
	s = fmt.Sprintf("%v_%v", b.Id, b.Lid)
	return s
}

func StringBallotSlot(b BallotNum, s int) string {
	s = fmt.Sprintf("%v_%v_%v", b.Id, b.Lid, s)
}

func HashAddr(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	ha := h.Sum(nil)
	id := big.Int{}
	id.SetBytes(ha[:length])
	return id.String()
}
