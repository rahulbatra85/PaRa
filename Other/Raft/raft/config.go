package raft

import (
	"fmt"
)

/*
const (
	HeartbeatFrequency  = 200
	ElectionTimeout     = 800
	ElectionTimeoutBase = 300
	RPCTimeout          = 500
	ClientRPCTimeout    = 1000
	DialTimeout         = 100
)*/

const (
	HeartbeatFrequency  = 2000
	ElectionTimeout     = 4000
	ElectionTimeoutBase = 4000
	RPCTimeout          = 1000
	ClientRPCTimeout    = 10000
	DialTimeout         = 100
)

type RaftConfig struct {
	NodeIdSize          int
	ClusterSize         int
	Majority            int
	HeartbeatFrequency  int //in ms
	ElectionTimeout     int //in ms
	ElectionTimeoutBase int
	RPCCancelTimeout    int
}

func CreateRaftConfig() *RaftConfig {
	c := new(RaftConfig)
	c.NodeIdSize = 2
	c.ClusterSize = 3
	c.Majority = (c.ClusterSize + 1) / 2
	c.HeartbeatFrequency = HeartbeatFrequency
	c.ElectionTimeout = ElectionTimeout
	c.ElectionTimeoutBase = ElectionTimeoutBase
	c.RPCCancelTimeout = 500
	return c
}

type RaftClientConfig struct {
	NodeIdSize       int
	ClusterSize      int
	RPCCancelTimeout int
}

func CreateRaftClientConfig() *RaftClientConfig {
	rc := new(RaftClientConfig)
	rc.NodeIdSize = 2
	rc.ClusterSize = 3
	rc.RPCCancelTimeout = 500

	return rc
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
