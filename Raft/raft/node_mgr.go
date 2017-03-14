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
	connState   map[NodeAddr]bool
	listener    net.Listener
	RPCServer   *NodeManagerRPCServer
	config      *NodeManagerConfig
	mu          sync.Mutex
}

func MakeNodeManager(port int, config *NodeManagerConfig) (pnm *NodeManager) {
	var nm NodeManager
	pnm = &nm

	nm.config = config
	nm.connState = make(map[NodeAddr]bool)

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

	//go nm.run()

	return pnm
}

func (nm *NodeManager) Run() {
	//Wait until received msg from all nodes in the cluster
	for len(nm.serversAddr) < nm.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	nm.INF("All Nodes have notified\n")
	for i, n := range nm.serversAddr {
		nm.INF("Node[%d]=[%v] %v\n", i, n.Id, n.Addr)
	}

	if nm.TestBasicElection() == false {
		nm.IdleLoop()
	}

}

func (nm *NodeManager) IdleLoop() {
	for {
		//Run tests
	}
}

func (nm *NodeManager) TestBasicElection() bool {
	nm.INF("Test Basic Election")
	//Only one node should be leader

	time.Sleep(1000 * time.Millisecond)
	if nm.checkOneLeader() == false {
		nm.INF("FAIL: More than one leader")
		return false
	}
	//Get state
	//check that terms haven't changed
	nm.INF("Test Basic Election PASSED!\n\n")
	return true
}

func (nm *NodeManager) TestReElection() {
	//Get state
	//check only one leader, and get current leader1
	//disconnect(leader1)
	//check only one leader, and get current leader2
	//connect(leader1) //old leader
	//check only one leader and that it's still leader2

	//disconnect(leader2)
	//disconnect(leader2+1%clusterSize)
	//check no leader

	//connect(leader2_1%clusterSize)
	//check only one leader

	//connect(leader2)
	//check only one leader, and it's still leader2+1%clusterSize)
}

func (nm *NodeManager) TestBasicAgree() {
}

func (nm *NodeManager) TestFailAgree() {
}

func (nm *NodeManager) TestFailNoAgree() {
}

func (nm *NodeManager) TestRejoin() {
}

//checkTerms
//Get terms
//If terms for each node don't match, then fail

//checkOneLeader
func (nm *NodeManager) checkOneLeader() bool {
	cnt := 0
	for _, n := range nm.serversAddr {
		if nm.connState[n] == true {
			//Get state
			err, state := GetStateRPC(&n)
			if err == nil && state == LEADER {
				cnt++
			}
		}
	}
	//Check that only one node is a leader
	if cnt != 1 {
		nm.INF("Leader Cnt=%d\n", cnt)
		return false
	} else {
		return true
	}
}

//checkNoLeader
//Get state
//Check that no node is a leader

//checkLogs
//Get state
//check that log for each node matches

//connect(NodeAddr)
//EnableNodeRPC

//disconnect
//DisableNodeRPC
