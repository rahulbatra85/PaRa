package raft

import (
	"fmt"
	"net"
	"os"
	"strconv"
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

func CreateUnixListener(port int) (net.Listener, error) {
	sf := "/var/tmp/raft-"
	sf += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(sf, 0777)
	sf += "rf-"
	sf += strconv.Itoa(os.Getpid()) + "-"
	sf += strconv.Itoa(port)

	conn, err := net.Listen("unix", sf)

	return conn, err
}
