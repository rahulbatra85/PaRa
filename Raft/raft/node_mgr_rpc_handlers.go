package raft

import (
	"fmt"
)

func (nm *NodeManager) ReadyNotification(request *ReadyNotificationRequest) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.INF("ReadyNotification from %v\n", request.FromAddr)

	if len(nm.serversAddr) == nm.config.ClusterSize {
		return fmt.Errorf("Node Notified after all nodes have notified")
	} else {
		nm.serversAddr = append(nm.serversAddr, request.FromAddr)
		nm.connState[request.FromAddr] = true
	}

	return nil
}
