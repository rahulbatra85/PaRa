package paxos

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type KVClient struct {
	Id        int
	SeqNum    int
	Replicas  []NodeAddr
	listener  net.Listener
	RPCServer *ClientRPCServer
	mu        sync.Mutex
}

func MakeKVClient(cid int, port int, replicas []NodeAddr) *KVClient {
	var kvc KVClient
	kvc.Id = cid
	kvc.SeqNum = 1
	kvc.Replicas = append(kvc.Replicas, replicase...)

	ClientInitTracers()
	conn, err := CreateListener(port)
	if err != nil {
		fmt.Printf("Error Creating Listener=%v\n", err)
		return
	}
	kvc.listener = conn

	kvc.RPCServer = &ClientRPCServer{kvc}
	rpc.RegisterName(conn.Addr().String(), kvc.RPCServer)
	go kvc.RPCServer.startClientRPCServer()

	return &kvc
}

func (kvc *KVClient) SendRequest(opType string, key string, data string) {
	op := Operation{OpType: optype, Key: key, Data: data}
	cmd := Command{Cid: kvc.Id, SeqNum: kvc.SeqNum, Op: op}

	req := ClientRequest{Cmd: cmd}
	for n := range Replicas {
		go ClientRequestRPC(n, req)
	}

}
