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

	//if nm.TestBasicElection() == false {
	//	nm.IdleLoop()
	//}

	if nm.TestReElection() == false {
		nm.IdleLoop()
	}

}

func (nm *NodeManager) IdleLoop() {
	for {
	}
}

func (nm *NodeManager) TestBasicElection() bool {
	nm.INF("Test Basic Election")
	//Only one node should be leader

	time.Sleep(1000 * time.Millisecond)
	if ok, _ := nm.checkOneLeader(); !ok {
		nm.INF("FAIL: More than one leader or no leader")
		return false
	}
	//Get state
	//check that terms haven't changed
	nm.INF("Test Basic Election PASSED!\n\n")
	return true
}

func (nm *NodeManager) TestReElection() bool {
	nm.INF("Test Reelection")

	//check only one leader, and get current leader1
	time.Sleep(1000 * time.Millisecond)
	var leader1 NodeAddr
	var leader2 NodeAddr
	var leader NodeAddr
	var ok bool
	if ok, leader1 = nm.checkOneLeader(); !ok {
		nm.INF("FAIL: More than one leader or no leader")
		return false
	}
	//disconnect(leader1)
	//check only one leader, and get current leader2
	//connect(leader1) //old leader
	//check only one leader and that it's still leader2
	nm.disconnect(&leader1)
	time.Sleep(2000 * time.Millisecond)
	if ok, leader2 = nm.checkOneLeader(); !ok {
		nm.INF("FAIL: More than one leader or no leader")
		return false
	}
	if leader2 == leader1 {
		nm.INF("FAIL: Leader1 is still leader")
	}
	nm.connect(&leader1)
	if ok, leader = nm.checkOneLeader(); !ok {
		nm.INF("FAIL: More than one leader or no leader")
		return false
	}
	if leader2 != leader {
		nm.INF("FAIL: Old leader reconnect affected currn leader")
		return false
	}

	//disconnect(leader2)
	//disconnect(leader2+1%clusterSize)
	//check no leader

	//connect(leader2_1%clusterSize)
	//check only one leader

	//connect(leader2)
	//check only one leader, and it's still leader2+1%clusterSize)

	nm.INF("Test ReElection PASSED!\n\n")
	return true
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
func (nm *NodeManager) checkOneLeader() (bool, NodeAddr) {
	cnt := 0
	var leader NodeAddr
	for _, n := range nm.serversAddr {
		if nm.connState[n] == true {
			//Get state
			state, err := GetStateRPC(&n)
			if err == nil && state == RaftState_LEADER {
				cnt++
				leader = n
			}
		}
	}
	//Check that only one node is a leader
	if cnt != 1 {
		nm.INF("Leader Cnt=%d\n", cnt)
		return false, leader
	} else {
		return true, leader
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
func (nm *NodeManager) connect(s *NodeAddr) {
	err := EnableNodeRPC(s)
	if err != nil {

	}
}

//disconnect
//DisableNodeRPC
func (nm *NodeManager) disconnect(s *NodeAddr) {
	err := DisableNodeRPC(s)
	if err != nil {

	}

}
