package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"./raft"
)

type nAddr []string

func (na *nAddr) String() string {
	return fmt.Sprintf("%v", *na)
}

func (na *nAddr) Set(value string) error {
	*na = append(*na, value)

	return nil
}

func main() {

	var addrs nAddr
	var clientID int
	flag.Var(&addrs, "l", "Nodes Addresses. Must not be empty")
	flag.IntVar(&clientID, "i", -1, "ClientID")
	flag.Parse()

	if addrs == nil {
		fmt.Fprintf(os.Stderr, "Nodes must be specified")
		return
	}

	Nodes := make([]raft.NodeAddr, len(addrs))
	for i, addr := range addrs {
		Nodes[i] = raft.NodeAddr{Id: raft.HashAddr(addr, 2), Addr: addr}
	}

	//create client
	config := raft.CreateRaftClientConfig()
	rc := raft.MakeRaftClient(Nodes, config, int32(clientID))
	if rc == nil {
		fmt.Fprintf(os.Stderr, "Client Create Failed. Exiting")
	} else {
		fmt.Printf("Ready for input commands\n")
		inbuf := bufio.NewReader(os.Stdin)
		for {
			//parse command
			input, err := inbuf.ReadString('\n')
			if err == io.EOF {
				continue
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "Error Reading input")
				continue
			}

			input = strings.TrimSpace(input)
			if input == "" {
				continue
			}
			tokens := strings.Split(input, " ")
			if tokens[0] == "exit" {
				break
			} else if tokens[0] == "GET" || tokens[0] == "get" {
				if len(tokens) != 2 {
					fmt.Fprintf(os.Stderr, "Invalid Command %s. Expected GET/get <key>\n", input)
				} else {
					result, err := rc.SendClientGetRequest(tokens[1])
					if err == nil {
						fmt.Printf("%s was successful. value=%s \n", input, result)
					} else {
						fmt.Fprintf(os.Stderr, "%v\n", err)
					}
				}
			} else if tokens[0] == "PUT" || tokens[0] == "put" {
				if len(tokens) != 3 {
					fmt.Fprintf(os.Stderr, "Invalid Command %s. Expected PUT/put <key> <value>\n", input)
				} else {
					err := rc.SendClientPutRequest(tokens[1], tokens[2])
					if err == nil {
						fmt.Printf("%s was successful \n", input)
					} else {
						fmt.Fprintf(os.Stderr, "%v\n", err)
					}
				}
			}
		}
	}
}
