package main

import (
	"./paxos"
	"flag"
	"fmt"
)

func main() {
	var cAddr string
	var port int

	flag.IntVar(&port, "port", 0, "Server Port. Default is random.")
	flag.StringVar(&cAddr, "cAddr", "", "Addr of Cluster Node. Default is empty.")

	flag.Parse()

	config := paxos.MakePaxosConfig()

	var remoteNodeAddr *paxos.NodeAddr
	if cAddr != "" {
		remoteNodeAddr = &paxos.NodeAddr{Id: paxos.HashAddr(cAddr, config.NodeIdSize), Addr: cAddr}
	}

	p := paxos.MakePaxos(port, remoteNodeAddr, config)

	fmt.Printf("Creating Paxos Node. Port=%v, cAddr=%v\n", port, cAddr)
	fmt.Printf("%v\n", p)

	//Now just loop forever
	for {
	}
}
