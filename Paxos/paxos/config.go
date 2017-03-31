package paxos

type PaxosConfig struct {
	NodeIdSize  int
	ClusterSize int
}

func MakePaxosConfig() *PaxosConfig {
	c := new(PaxosConfig)
	c.NodeIdSize = 2
	c.ClusterSize = 3
	return c
}

type PaxosClientKVConfig struct {
	ClusterSize int
	NodeIdSize  int
}

func MakePaxosClientKVConfig() *PaxosClientKVConfig {
	c := new(PaxosClientKVConfig)
	c.ClusterSize = 3
	c.NodeIdSize = 2

	return c
}
