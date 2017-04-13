//Name: client.go
//Description: Implements Raft Key-Value Client
//Author: Rahul Batra

package raft_kv

import (
	"fmt"
	"net/rpc"
)

type RaftClient struct {
	servers   []string
	seqNum    int
	leaderIdx int
	id        int
}

func MakeRaftClient(servers []string, id int) *RaftClient {
	rc := new(RaftClient)
	rc.servers = servers
	rc.id = id
	return rc
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server.
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
		ok := call(rc.servers[rc.leaderIdx], "RaftKVServer.Get", &args, &reply)
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
		ok := call(rc.servers[rc.leaderIdx], "RaftKVServer.PutAppend", &args, &reply)
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
