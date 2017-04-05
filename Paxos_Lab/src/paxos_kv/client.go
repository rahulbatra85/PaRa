//Name: client.go
//Description: Implements Paxos Key-Value Client
//Author: Rahul Batra

package paxos_kv

import "net/rpc"

import "fmt"

type PaxosClient struct {
	servers []string
	id      int
	SeqNum  int
}

func MakePaxosClient(servers []string, id int) *PaxosClient {
	pc := new(PaxosClient)
	pc.servers = servers
	pc.id = id
	return pc
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
func (pc *PaxosClient) Get(key string) string {
	pc.SeqNum++
	args := GetArgs{}
	args.Key = key
	args.ReqID = pc.SeqNum
	args.ClientID = pc.id
	reply := &GetReply{}

	done := false
	idx := 0
	var retval string
	for !done {
		DPrintf("\tTrying GET Request to server[%d].\n", idx)
		ok := call(pc.servers[idx], "PaxosKVServer.Get", args, reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", idx)
			idx = (idx + 1) % len(pc.servers)
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

func (pc *PaxosClient) PutAppend(key string, value string, op string) {
	pc.SeqNum++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ReqID = pc.SeqNum
	args.ClientID = pc.id
	reply := &PutAppendReply{}

	done := false
	idx := 0
	for !done {
		DPrintf("\tTrying PUT Request to server[%d].\n", idx)
		ok := call(pc.servers[idx], "PaxosKVServer.PutAppend", args, reply)
		if !ok {
			DPrintf("\tRESP: Request to server[%d] Failed.\n", idx)
			idx = (idx + 1) % len(pc.servers)
		} else {
			done = true
			DPrintf("\tRESP: Request Successful\n")
		}
	}
}

// This function is for the client to put a value
// corresponding to a key.
func (pc *PaxosClient) Put(key string, value string) {
	pc.PutAppend(key, value, "Put")
}

// This function is for the client to append a value
// corresponding to a key.
func (pc *PaxosClient) Append(key string, value string) {
	pc.PutAppend(key, value, "Append")
}
