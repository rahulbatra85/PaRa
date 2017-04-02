package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqNum    int
	leaderIdx int
	id        int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, id int) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = id
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.ReqId = int(nrand())
	args.ClientId = ck.id
	reply := GetReply{}

	done := false
	var retval string
	for !done {
		DPrintf("\tTrying GET Request to server[%d].\n", ck.leaderIdx)
		ok := ck.servers[ck.leaderIdx].Call("RaftKV.Get", &args, &reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", ck.leaderIdx)
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
		} else {
			DPrintf("\tRESP: reply=%v\n", reply)
			if reply.WrongLeader == 1 {
				DPrintf("\tRESP: Server[%d] is not leader\n", ck.leaderIdx)
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			} else {
				done = true
				if reply.Err == OK {
					DPrintf("\tRESP: Request Successful\n")
					retval = reply.Value
				} else {
					DPrintf("\tRESP: Invalid Key\n")
					retval = ""
				}
			}
		}
	}

	return retval
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ReqId = int(nrand())
	args.ClientId = ck.id
	reply := PutAppendReply{}

	done := false
	for !done {
		DPrintf("\tTrying PUT Request to server[%d].\n", ck.leaderIdx)
		ok := ck.servers[ck.leaderIdx].Call("RaftKV.PutAppend", &args, &reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", ck.leaderIdx)
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
		} else {
			DPrintf("\tRESP: reply=%v\n", reply)
			if reply.WrongLeader == 1 {
				DPrintf("\tRESP: Server[%d] is not leader\n", ck.leaderIdx)
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			} else {
				done = true
				DPrintf("\tRESP: Request Successful\n")
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
