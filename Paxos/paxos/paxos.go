package paxos

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
	mu         sync.Mutex
	Id         string          //Id (hashed address)
	listener   net.Listener    //Listener
	port       int             //ListenerPort
	localAddr  NodeAddr        //LocalAddr
	othersAddr []NodeAddr      //OtherAddrs
	config     *PaxosConfig    //Config
	RPCServer  *PaxosRPCServer //Pointer to RPC server

	//Replica, Leader, and Acceptor
	r *Replica
	l *Leader
	a *Acceptor

	//Application
	app *KVApp
}

func MakePaxos(port int, remoteNodeAddr *NodeAddr, config *PaxosConfig) (pp *PaxosNode) {
	//PaxosNode
	var p PaxosNode
	pp = &p

	//Set config
	p.config = config

	//app state
	p.app = MakeKVApp()

	//Set up logging
	InitTracers()

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

	return pp
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

	go p.run()
}
