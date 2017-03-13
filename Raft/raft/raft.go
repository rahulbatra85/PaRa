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
	Id          string       //Id (hash address)
	listener    net.Listener //Listener
	port        int          //ListenerPort
	localAddr   NodeAddr     //LocalAddr
	othersAddr  []NodeAddr   //OtherAddrs
	nodeMgrAddr NodeAddr     //Node Manager
	mu          sync.Mutex
	stmu        sync.RWMutex
	//	app         *Application

	config    *RaftConfig //Config
	RPCServer *RaftRPCServer

	// Raft paper's Figure 2 description of state
	state     RaftState
	nextState RaftState

	//Persistent State on all servers
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  map[NodeAddr]int
	matchIndex map[NodeAddr]int

	//channels
	appendEntriesMsgCh   chan AppendEntriesMsg
	requestVoteMsgCh     chan RequestVoteMsg
	appendEntriesReplyCh chan AppendEntriesReply
	requestVoteReplyCh   chan RequestVoteReply

	//Application State
}

func MakeRaft(port int, remoteNodeAddr *NodeAddr, nodeMgrAddr *NodeAddr, config *RaftConfig) (pr *RaftNode) {

	//RaftNode
	var r RaftNode
	pr = &r

	//Set config
	r.config = config

	//init RaftNode variables
	r.commitIndex = 0
	r.lastApplied = 0
	r.nextIndex = make(map[NodeAddr]int)
	r.matchIndex = make(map[NodeAddr]int)
	r.state = FOLLOWER
	r.CurrentTerm = 0
	r.VotedFor = ""

	//Init channels
	r.appendEntriesMsgCh = make(chan AppendEntriesMsg)
	r.requestVoteMsgCh = make(chan RequestVoteMsg)
	r.appendEntriesReplyCh = make(chan AppendEntriesReply)
	r.requestVoteReplyCh = make(chan RequestVoteReply)

	if nodeMgrAddr != nil {
		r.nodeMgrAddr = *nodeMgrAddr
	} else {
		r.nodeMgrAddr.Id = ""
		r.nodeMgrAddr.Addr = ""
	}

	//Set up logging
	InitTracers()
	//SetDebugTrace(true)

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
	entry := LogEntry{Term: 0, Cmd: nil}
	r.AppendLog(entry)

	//Register and Start RPC server
	r.RPCServer = &RaftRPCServer{pr}
	rpc.RegisterName(r.localAddr.Addr, r.RPCServer)
	r.DBG("Registered RPC\n")
	go r.RPCServer.startRaftRPCServer()

	//Either Send JoinRPC to main node or wait to receive JoinRPC from all nodes
	if remoteNodeAddr != nil {
		r.INF("Sending JOIN RPC")
		err = JoinRPC(remoteNodeAddr, &r.localAddr)

	} else {
		go r.startNodes()
	}

	return
}

func (r *RaftNode) startNodes() {
	r.INF("Waiting until other nodes are up")

	r.mu.Lock()
	r.othersAddr = append(r.othersAddr, r.localAddr)
	r.mu.Unlock()

	//Wait until all other nodes have sent JoinRPC
	for len(r.othersAddr) < r.config.ClusterSize {
		time.Sleep(time.Millisecond * 100)
	}

	r.INF("OtherAddrs=%v", r.othersAddr)
	for _, otherNode := range r.othersAddr {
		if r.Id != otherNode.Id {
			fmt.Printf("(%v) Starting node-%v\n", r.Id, otherNode.Id)
			StartRPC(&otherNode, r.othersAddr)
		}
	}

	if r.nodeMgrAddr.Id != "" && r.nodeMgrAddr.Addr != "" {
		ReadyNotificationRPC(&r.nodeMgrAddr, &r.localAddr)
	}

	go r.run_server()
}
