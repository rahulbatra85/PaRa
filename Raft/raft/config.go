package raft

type RaftConfig struct {
	NodeIdSize  int
	ClusterSize int
}

func CreateRaftConfig() *RaftConfig {
	c := new(RaftConfig)
	c.NodeIdSize = 2
	c.ClusterSize = 3
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
