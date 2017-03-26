package raft

import (
	"fmt"
)

type RaftConfig struct {
	NodeIdSize          int
	ClusterSize         int
	Majority            int
	HeartbeatFrequency  int //in ms
	ElectionTimeout     int //in ms
	ElectionTimeoutBase int
}

func CreateRaftConfig() *RaftConfig {
	c := new(RaftConfig)
	c.NodeIdSize = 2
	c.ClusterSize = 3
	c.Majority = (c.ClusterSize + 1) / 2
	c.HeartbeatFrequency = 1000
	c.ElectionTimeout = 4000
	c.ElectionTimeoutBase = 6000
	return c
}

type NodeManagerConfig struct {
	ClusterSize int
}

func CreateNodeManagerConfig() *NodeManagerConfig {
	c := new(NodeManagerConfig)
	c.ClusterSize = 3

	return c
}

type RaftNetworkConfig struct {
	NodeConfig    map[string]bool
	NetworkEnable bool
}

func CreateRaftNetworkConfig() *RaftNetworkConfig {
	var rnc RaftNetworkConfig
	rnc.NodeConfig = make(map[string]bool)

	return &rnc
}

func (rnc *RaftNetworkConfig) EnableNetwork() {
	fmt.Printf("Network Enable\n")
	rnc.NetworkEnable = true
}

func (rnc *RaftNetworkConfig) DisableNetwork() {
	fmt.Printf("Network Disable\n")
	rnc.NetworkEnable = false
}

func (rnc *RaftNetworkConfig) GetNetworkConfig(n1 NodeAddr, n2 NodeAddr) bool {
	if rnc.NetworkEnable == false {
		//	fmt.Printf("Network Disable\n")
		return false
	}
	key := fmt.Sprintf("%v_%v", n1.Id, n2.Id)
	val, ok := rnc.NodeConfig[key]
	//fmt.Printf("%s %v\n", key, val)
	return ok && val
}

func (rnc *RaftNetworkConfig) SetNetworkConfig(n1 NodeAddr, n2 NodeAddr, val bool) {
	key := fmt.Sprintf("%v_%v", n1.Id, n2.Id)
	rnc.NodeConfig[key] = val
}
