package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type NodeManager struct {
	serversAddr []NodeAddr
	listener    net.Listener
	RPCServer   *NodeManagerRPCServer
	config      *NodeManagerConfig
	mu          sync.Mutex
}

func MakeNodeManager(port int, config *NodeManagerConfig) (pnm *NodeManager) {
	var nm NodeManager
	pnm = &nm

	nm.config = config

	NodeMgrInitTracers()
	conn, err := CreateListener(port)
	if err != nil {
		fmt.Printf("Error Creating Listener =%v\n", err)
		return
	}
	nm.listener = conn //Set listener

	nm.INF("Created listener %v\n", nm.listener.Addr().String())

	nm.RPCServer = &NodeManagerRPCServer{pnm}
	rpc.RegisterName(conn.Addr().String(), nm.RPCServer)

	go nm.RPCServer.startNodeManagerRPCServer()

	go nm.run()

	return pnm
}

func (nm *NodeManager) run() {
	//Wait until received msg from all nodes in the cluster
	for len(nm.serversAddr) < nm.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	nm.INF("All Nodes have notified\n")
	for i, n := range nm.serversAddr {
		nm.INF("Node[%d]=[%v] %v\n", i, n.Id, n.Addr)
	}

	for {
		//Run tests
	}
}
