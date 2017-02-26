package paxos

type PaxosConfig struct {
	NodeIdSize  int
	ClusterSize int
}

func CreatePaxosConfig() *PaxosConfig {
	c := new(PaxosConfig)
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
