package paxos

import (
	"fmt"
	"net/rpc"
)

var clientConn = make(map[string]*rpc.Client)

//Response RPC
type ResponseRequest struct {
	Cmd    Command
	Result string //\todo make it generic
}
type ResponseReply struct {
	Success bool
}

func ResponseRPC(remoteNode *NodeAddr, req ResponseRequest) error {
	var reply ResponseReply
	err := makeClientRemoteCall(remoteNode, "ResponseWrapper", req, &reply)
	if err != nil {
		return err
	}

	if !reply.Success {
		return fmt.Errorf("Client Response failed")
	}

	return nil
}

func makeClientRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}) error {
	var err error
	client, ok := clientConn[remoteAddr.Addr]
	if !ok {
		client, err := rpc.Dial("tcp", remoteAddr.Addr)
		if err != nil {
			return err
		}
		clientConn[remoteAddr.Addr] = client
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	err = client.Call(fullProcName, request, reply)
	if err != nil {
		delete(clientConn, remoteAddr.Addr)
	}
}
