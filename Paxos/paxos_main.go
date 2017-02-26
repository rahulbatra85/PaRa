package main

import (
	"./paxos"
	"flag"
	"fmt"
)

func main() {
	var cAddr string
	var nmAddr string
	var port int

	flag.IntVar(&port, "port", 0, "Server Port. Default is random.")
	flag.StringVar(&cAddr, "cAddr", "", "Addr of Cluster Node. Default is empty.")
	flag.StringVar(&nmAddr, "nmAddr", "", "Addr of Node Manager")

	flag.Parse()

	config := paxos.CreatePaxosConfig()

	var remoteNodeAddr *paxos.NodeAddr
	if cAddr != "" {
		remoteNodeAddr = &paxos.NodeAddr{Id: paxos.HashAddr(cAddr, config.NodeIdSize), Addr: cAddr}
	}

	var nodeMgrAddr *paxos.NodeAddr
	if nmAddr != "" {
		nodeMgrAddr = &paxos.NodeAddr{Id: paxos.HashAddr(nmAddr, config.NodeIdSize), Addr: nmAddr}
	}

	p := paxos.MakePaxos(port, remoteNodeAddr, nodeMgrAddr, config)

	fmt.Printf("Creating Paxos Node. Port=%v, cAddr=%v\n", port, cAddr)
	fmt.Printf("%v\n", p)
	//\TODO add interaction with NodeManager

	//Now just loop forever
	for {
	}
}
