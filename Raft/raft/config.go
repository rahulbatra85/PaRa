package raft

type RaftConfig struct {
	NodeIdSize         int
	ClusterSize        int
	Majority           int
	HeartbeatFrequency int //in ms
	ElectionTimeout    int //in ms
}

func CreateRaftConfig() *RaftConfig {
	c := new(RaftConfig)
	c.NodeIdSize = 2
	c.ClusterSize = 3
	c.Majority = (c.ClusterSize + 1) / 2
	c.HeartbeatFrequency = 150
	c.ElectionTimeout = 400
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
