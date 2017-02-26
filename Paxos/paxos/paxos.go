package paxos

//
//Note: The startup code is derived and adapted from labs assignments of MIT 6.824 and Brown CS 138 courses
//These are distributed systems courses.
//

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type PaxosNode struct {
	mu          sync.Mutex
	Id          string       //Id (hash address)
	listener    net.Listener //Listener
	port        int          //ListenerPort
	localAddr   NodeAddr     //LocalAddr
	othersAddr  []NodeAddr   //OtherAddrs
	nodeMgrAddr NodeAddr     //Node Manager

	config    *PaxosConfig //Config
	RPCServer *PaxosRPCServer
	//Pointer to RPC server
	//NodeRPC policy

	//Data
	ballotNum int

	//Channels

	//Application State
}

type NodeAddr struct {
	Id   string
	Addr string
}

func MakePaxos(port int, remoteNodeAddr *NodeAddr, nodeMgrAddr *NodeAddr, config *PaxosConfig) (pp *PaxosNode) {
	//Inputs: Port, RemoteAddr, Config

	//PaxosNode
	var p PaxosNode
	pp = &p

	//Set config
	p.config = config

	//init PaxosNode variables
	//Init channels
	//app state
	//etc
	p.ballotNum = 0
	if nodeMgrAddr != nil {
		p.nodeMgrAddr = *nodeMgrAddr
	} else {
		p.nodeMgrAddr.Id = ""
		p.nodeMgrAddr.Addr = ""
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

	p.Id = HashAddr(conn.Addr().String(), p.config.NodeIdSize) //Hash Addr to determine ID
	p.INF("Created Listener %v\n", conn.Addr())
	p.listener = conn //Set listener
	p.port = port     //Set listen Port
	p.localAddr = NodeAddr{Id: p.Id, Addr: conn.Addr().String()}

	//Init stable storage

	//Register and Start RPC server
	p.RPCServer = &PaxosRPCServer{pp}
	rpc.RegisterName(p.localAddr.Addr, p.RPCServer)
	p.DBG("Registered RPC\n")
	go p.RPCServer.startPaxosRPCServer()

	//Either Send JoinRPC to main node or wait to receive JoinRPC from all nodes
	if remoteNodeAddr != nil {
		err = JoinRPC(remoteNodeAddr, &p.localAddr)
	} else {
		go p.startNodes()
	}

	return
}

func (p *PaxosNode) startNodes() {
	p.mu.Lock()
	p.othersAddr = append(p.othersAddr, p.localAddr)
	p.mu.Unlock()

	for len(p.othersAddr) < p.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	for _, otherNode := range p.othersAddr {
		if p.Id != otherNode.Id {
			fmt.Printf("(%v) Starting node-%v\n", p.Id, otherNode.Id)
			StartRPC(&otherNode, p.othersAddr)
		}
	}

	if p.nodeMgrAddr.Id != "" && p.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&p.nodeMgrAddr, &p.localAddr)
	}

	go p.run()
}

func HashAddr(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	ha := h.Sum(nil)
	id := big.Int{}
	id.SetBytes(ha[:length])
	return id.String()
}
