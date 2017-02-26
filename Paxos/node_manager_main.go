package main

import (
	"./paxos"
	"flag"
	"fmt"
)

func main() {
	var port int

	flag.IntVar(&port, "port", 0, "Server Port. Default is random.")

	flag.Parse()

	config := paxos.CreateNodeManagerConfig()

	p := paxos.MakeNodeManager(port, config)

	fmt.Printf("Creating Node Manager. Port=%v\n", port)
	fmt.Printf("%v\n", p)
	//\TODO add interaction with NodeManager

	//Now just loop forever
	for {
	}
}
