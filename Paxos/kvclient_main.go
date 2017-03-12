package main

import (
	"./paxos"
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

type addrSlice []string

func (a *addrSlice) String() string {
	return fmt.SPrintf("%v", *addrSlice)
}

func (a *addrSlice) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	var rAddr addrSlice
	var port int
	var cid int

	flag.IntVar(&port, "port", 0, "Client Port. Default is random.")
	flag.StringVar(&rAddr, "rAddrs", "", "Addresses of Replica. Must not be empty.")
	flag.IntVar(&cid, "cid", 0, "Client-ID. Make sure all client-ids are unique")

	flag.Parse()

	config := paxos.CreatePaxosConfig()

	if len(rAddr) != config.ClusterSize {
		log.Fatalf("Not enough replicas specified)")
	} else {
		kvc := paxos.MakeKVClient(cid, port, rAddr)

		//Process user cmds
		for {
			input := bufio.NewScanner(os.Stdin)
			for input.Scan() {
				s := strings.Split(input.Text(), " ")
				if len(s) < 2 || len(s) > 3 {
					fmt.Printf("Invalid Syntax:=%v. Cmd must be [op] [key] [data]\n")
				} else {
					kvc.SendRequest(len[0], len[1], len[2])
				}
			}
		}
	}
}
