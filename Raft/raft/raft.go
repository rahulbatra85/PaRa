package raft

//
//Note: The startup code is derived and adapted from labs assignments of MIT 6.824 and Brown CS 138 courses
//These are distributed systems courses.
//

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type RaftNode struct {
	mu          sync.Mutex
	Id          string       //Id (hash address)
	listener    net.Listener //Listener
	port        int          //ListenerPort
	localAddr   NodeAddr     //LocalAddr
	othersAddr  []NodeAddr   //OtherAddrs
	nodeMgrAddr NodeAddr     //Node Manager
	//	app         *Application

	config    *RaftConfig //Config
	RPCServer *RaftRPCServer

	//Channels

	//Application State
}

func MakeRaft(port int, remoteNodeAddr *NodeAddr, nodeMgrAddr *NodeAddr, config *RaftConfig) (pr *RaftNode) {
	//Inputs: Port, RemoteAddr, Config

	//RaftNode
	var r RaftNode
	pr = &r

	//Set config
	r.config = config

	//init RaftNode variables
	//Init channels
	//app state
	//etc
	//r.app = app
	if nodeMgrAddr != nil {
		r.nodeMgrAddr = *nodeMgrAddr
	} else {
		r.nodeMgrAddr.Id = ""
		r.nodeMgrAddr.Addr = ""
	}

	//Set up logging
	InitTracers()

	//Set communication

	//Create listener
	conn, err := CreateListener(port)
	if err != nil {
		fmt.Printf("Error Creating Listener =%v\n", err)
		return
	}

	r.Id = HashAddr(conn.Addr().String(), r.config.NodeIdSize) //Hash Addr to determine ID
	r.INF("Created Listener %v\n", conn.Addr())
	r.listener = conn //Set listener
	r.port = port     //Set listen Port
	r.localAddr = NodeAddr{Id: r.Id, Addr: conn.Addr().String()}

	//Init stable storage

	//Register and Start RPC server
	r.RPCServer = &RaftRPCServer{pr}
	rpc.RegisterName(r.localAddr.Addr, r.RPCServer)
	r.DBG("Registered RPC\n")
	go r.RPCServer.startRaftRPCServer()

	//Either Send JoinRPC to main node or wait to receive JoinRPC from all nodes
	if remoteNodeAddr != nil {
		err = JoinRPC(remoteNodeAddr, &r.localAddr)
	} else {
		go r.startNodes()
	}

	return
}

func (r *RaftNode) startNodes() {
	r.mu.Lock()
	r.othersAddr = append(r.othersAddr, r.localAddr)
	r.mu.Unlock()

	for len(r.othersAddr) < r.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	for _, otherNode := range r.othersAddr {
		if r.Id != otherNode.Id {
			fmt.Printf("(%v) Starting node-%v\n", r.Id, otherNode.Id)
			StartRPC(&otherNode, r.othersAddr)
		}
	}

	if r.nodeMgrAddr.Id != "" && r.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&r.nodeMgrAddr, &r.localAddr)
	}

	go r.run()
}
