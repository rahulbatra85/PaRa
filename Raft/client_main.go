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

func main() {

	var leaderAddr string
	flag.StringVar(&leaderAddr, "l", "", "Leader Node. Must not be empty ")
	flag.Parse()

	if leaderAddr == "" {
		fmt.Fprintf(os.Stderr, "LeaderAddr must be specified")
		return
	}

	leaderNode := raft.NodeAddr{Id: raft.HashAddr(leaderAddr, 2), Addr: leaderAddr}

	//create client
	rc := raft.MakeRaftClient(leaderNode)

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
				fmt.Fprintf(os.Stderr, "Invalid Command %s. Expected GET/get <key>", input)
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
				fmt.Fprintf(os.Stderr, "Invalid Command %s. Expected PUT/put <key> <value>", input)
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
