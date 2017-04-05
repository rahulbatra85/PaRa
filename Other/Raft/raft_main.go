package main

import (
	"flag"
	"fmt"
	"runtime"

	"./raft"
)

func main() {
	var cAddr string
	var nmAddr string
	var port int
	var eTO int
	runtime.GOMAXPROCS(2)

	flag.IntVar(&port, "port", 0, "Server Port. Default is random.")
	flag.StringVar(&cAddr, "cAddr", "", "Addr of Cluster Node. Default is empty.")
	flag.StringVar(&nmAddr, "nmAddr", "", "Addr of Node Manager")
	flag.IntVar(&eTO, "timeout", 0, "Election Timeout")

	flag.Parse()

	config := raft.CreateRaftConfig()
	if eTO > config.ElectionTimeout {
		config.ElectionTimeout = eTO
	}

	var remoteNodeAddr *raft.NodeAddr
	if cAddr != "" {
		remoteNodeAddr = &raft.NodeAddr{Id: raft.HashAddr(cAddr, config.NodeIdSize), Addr: cAddr}
	}

	var nodeMgrAddr *raft.NodeAddr
	if nmAddr != "" {
		nodeMgrAddr = &raft.NodeAddr{Id: raft.HashAddr(nmAddr, config.NodeIdSize), Addr: nmAddr}
	}

	r := raft.MakeRaft(port, remoteNodeAddr, nodeMgrAddr, config)

	fmt.Printf("Creating Raft Node. Port=%v, cAddr=%v\n", port, cAddr)
	fmt.Printf("%v\n", r)
	//\TODO add interaction with NodeManager

	//Now just loop forever
	for {
	}
}
