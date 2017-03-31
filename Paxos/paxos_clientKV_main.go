package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"./paxos"
)

type addrSlice []string

func (a *addrSlice) String() string {
	return fmt.Sprintf("%v", *a)
}

func (a *addrSlice) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	var rAddrs addrSlice
	var cid int

	flag.Var(&rAddrs, "rAddrs", "Addresses of Replica. Must not be empty.")
	flag.IntVar(&cid, "cid", 0, "Client-ID. Make sure all client-ids are unique")

	flag.Parse()

	config := paxos.MakePaxosClientKVConfig()

	if len(rAddrs) != config.ClusterSize {
		log.Fatalf("Not enough replicas specified)")
	} else {
		nodes := make([]paxos.NodeAddr, len(rAddrs))
		for i, addr := range rAddrs {
			nodes[i] = paxos.NodeAddr{Id: paxos.HashAddr(addr, config.NodeIdSize), Addr: addr}
		}
		kvc := paxos.MakePaxosClientKV(config, cid, nodes)

		//Process user cmds
		done := false
		for !done {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				input := scanner.Text()
				tokens := strings.Split(input, " ")
				//Exit
				if tokens[0] == "exit" || tokens[1] == "EXIT" {
					done = true
					break
					//GET
				} else if tokens[0] == "GET" || tokens[0] == "get" {
					if len(tokens) != 2 {
						fmt.Fprintf(os.Stderr, "Invalid Syntax %s. Expected GET/get <key>\n", input)
					} else {
						result, err := kvc.SendGETRequest(tokens[1])
						if err == nil {
							fmt.Printf("%s was successful. Result=%s\n", input, result)
						} else {
							fmt.Fprintf(os.Stderr, "%v\n", err)
						}
					}
					//PUT
				} else if tokens[0] == "PUT" || tokens[0] == "put" {
					if len(tokens) != 3 {
						fmt.Fprintf(os.Stderr, "Invalid Syntax %s. Expected GET/get <key>\n", input)
					} else {
						err := kvc.SendPUTRequest(tokens[1], tokens[2])
						if err == nil {
							fmt.Printf("%s was successful. \n", input)
						} else {
							fmt.Fprintf(os.Stderr, "%v\n", err)
						}
					}
				} else {
					fmt.Fprintf(os.Stderr, "Invalid Command%s. Cmd must be GET/PUT/EXIT\n")
				}
			}
		}
	}
}
