package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"

import "fmt"

type Clerk struct {
	servers []string
	id      int
	SeqNum  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string, id int) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = id
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.SeqNum++
	args := GetArgs{}
	args.Key = key
	args.ReqID = ck.SeqNum
	args.ClientID = ck.id
	reply := &GetReply{}

	done := false
	idx := 0
	var retval string
	for !done {
		DPrintf("\tTrying GET Request to server[%d].\n", idx)
		ok := call(ck.servers[idx], "KVPaxos.Get", args, reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", idx)
			idx = (idx + 1) % len(ck.servers)
		} else {
			done = true
			if reply.Err == OK {
				retval = reply.Value
				DPrintf("\tRESP: Request Successful\n")
				retval = reply.Value
			} else {
				DPrintf("\tRESP: Invalid Key\n")
				retval = ""
			}
		}
	}

	return retval
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.SeqNum++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ReqID = ck.SeqNum
	args.ClientID = ck.id
	reply := &PutAppendReply{}

	done := false
	idx := 0
	for !done {
		DPrintf("\tTrying PUT Request to server[%d].\n", idx)
		ok := call(ck.servers[idx], "KVPaxos.PutAppend", args, reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", idx)
			idx = (idx + 1) % len(ck.servers)
		} else {
			done = true
			DPrintf("\tRESP: Request Successful\n")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
