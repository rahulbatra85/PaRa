package main

import (
	"./raft"
	"flag"
	"fmt"
)

func main() {
	var port int

	flag.IntVar(&port, "port", 0, "Server Port. Default is random.")

	flag.Parse()

	config := raft.CreateNodeManagerConfig()

	nm := raft.MakeNodeManager(port, config)

	fmt.Printf("Creating Node Manager. Port=%v\n", port)
	fmt.Printf("%v\n", nm)

	nm.Run()

}
