package paxos

import (
	"fmt"
	"net"
	"os"
)

//This creates a TCP listener on the given port
func CreateListener(port int) (net.Listener, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%v:%v", hostname, port)
	conn, err := net.Listen("tcp4", addr)

	return conn, err
}
