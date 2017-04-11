//Name: client.go
//Description: Implements Raft Key-Value Client
//Author: Rahul Batra

package raft_kv

import "labrpc"

type RaftClient struct {
	servers   []*labrpc.ClientEnd
	seqNum    int
	leaderIdx int
	id        int
}

func MakeRaftClient(servers []*labrpc.ClientEnd, id int) *RaftClient {
	rc := new(RaftClient)
	rc.servers = servers
	rc.id = id
	return rc
}

// This function is for the client to obtain the
// value corresponding to a key. If key doesn't exist
// then it returns an empty value ""
func (rc *RaftClient) Get(key string) string {
	rc.seqNum++
	args := GetArgs{}
	args.Key = key
	args.ReqId = rc.seqNum
	args.ClientId = rc.id
	reply := GetReply{}

	done := false
	var retval string
	for !done {
		DPrintf("\tTrying GET Request to server[%d].\n", rc.leaderIdx)
		ok := rc.servers[rc.leaderIdx].Call("RaftKV.Get", &args, &reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", rc.leaderIdx)
			rc.leaderIdx = (rc.leaderIdx + 1) % len(rc.servers)
		} else {
			DPrintf("\tRESP: reply=%v\n", reply)
			if reply.WrongLeader == 1 {
				DPrintf("\tRESP: Server[%d] is not leader\n", rc.leaderIdx)
				rc.leaderIdx = (rc.leaderIdx + 1) % len(rc.servers)
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

func (rc *RaftClient) PutAppend(key string, value string, op string) {
	rc.seqNum++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ReqId = rc.seqNum
	args.ClientId = rc.id
	reply := PutAppendReply{}

	done := false
	for !done {
		DPrintf("\tTrying PUT Request to server[%d].\n", rc.leaderIdx)
		ok := rc.servers[rc.leaderIdx].Call("RaftKV.PutAppend", &args, &reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", rc.leaderIdx)
			rc.leaderIdx = (rc.leaderIdx + 1) % len(rc.servers)
		} else {
			DPrintf("\tRESP: reply=%v\n", reply)
			if reply.WrongLeader == 1 {
				DPrintf("\tRESP: Server[%d] is not leader\n", rc.leaderIdx)
				rc.leaderIdx = (rc.leaderIdx + 1) % len(rc.servers)
			} else {
				done = true
				DPrintf("\tRESP: Request Successful\n")
			}
		}
	}
}

// This function is for the client to put a value
// corresponding to a key.
func (rc *RaftClient) Put(key string, value string) {
	rc.PutAppend(key, value, "Put")
}

// This function is for the client to append a value
// corresponding to a key.
func (rc *RaftClient) Append(key string, value string) {
	rc.PutAppend(key, value, "Append")
}
