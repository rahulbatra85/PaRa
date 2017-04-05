package raft

import (
	"fmt"
	"time"
)

type RaftClient struct {
	Config   *RaftClientConfig
	ClientId int32
	Leader   NodeAddr
	Nodes    []NodeAddr
	SeqNum   uint64
	Conns    map[NodeAddr]*connection
}

func MakeRaftClient(nodes []NodeAddr, config *RaftClientConfig, clientID int32) *RaftClient {
	var rc RaftClient
	rc.Leader = nodes[0]
	rc.Config = config
	rc.Nodes = nodes

	//Register Client
	request := ClientRegisterArgs{}
	ClientInitTracers()
	rc.Conns = make(map[NodeAddr]*connection)
	for _, node := range rc.Nodes {
		rc.Conns[node] = MakeConnection(&node)
	}

	if clientID > 0 {
		rc.ClientId = clientID
		return &rc
	} else {
		fmt.Printf("Raft Client Created")
		tryCnt := 0
		done := false
		leaderIdx := 0
		for !done && tryCnt < 50 {
			fmt.Printf("\tTrying to register with %v, cnt=%d\n", rc.Leader, tryCnt)
			reply, err := rc.ClientRegisterRPC(&rc.Leader, request)
			if err != nil {
				fmt.Printf("\tRESP: Error sending RPC to %v. %v\n", rc.Leader, err)
				if leaderIdx < len(rc.Nodes) {
					leaderIdx++
					rc.Leader = rc.Nodes[leaderIdx]
				} else {
					fmt.Printf("\tRESP: Tried all nodes in the cluster. Giving up...\n")
					return nil
				}
			} else {
				if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {

					done = true
					rc.Leader = *(reply.LeaderNode)
					rc.ClientId = reply.ClientId
					fmt.Printf("\tRESP:Registration Successful. Client_ID=%d\n", rc.ClientId)
					return &rc
				} else if reply.Code == ClientReplyCode_NOT_LEADER {
					fmt.Printf("\tRESP:Contacted NOT_LEADER.\n")
					leaderIdx = 0
					if reply.LeaderNode != nil {
						rc.Leader = *(reply.LeaderNode)
					}
				} else if reply.Code == ClientReplyCode_RETRY {
					leaderIdx = 0
					fmt.Printf("\tRESP:RegistrationRETRY\n")
					time.Sleep(time.Millisecond * 500)
				} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
					fmt.Printf("RESP:Registration REQUEST_FAILED\n")
					done = true
					return nil
				}
			}
			tryCnt++
		}
	}

	return nil
}

func (rc *RaftClient) SendClientGetRequest(key string) (string, error) {
	opr := Operation{Type: OpType_GET, Key: key, Value: ""}
	cmd := Command{ClientId: rc.ClientId, SeqNum: rc.SeqNum, Op: &opr}
	request := ClientRequestArgs{Cmd: &cmd}

	fmt.Printf("Sending GET Request\n")
	tryCnt := 0
	leaderIdx := 0
	rc.SeqNum++
	for {
		fmt.Printf("\n\tTrying GET Request to %v\n", rc.Leader)
		reply, err := rc.ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			fmt.Printf("\tRESP: Error sending RPC to %v. %v\n", rc.Leader, err)
			/*if leaderIdx < len(rc.Nodes) {
				rc.Leader = rc.Nodes[leaderIdx]
			} else {
				return "", fmt.Errorf("Tried all nodes in the cluster")
			}*/
			leaderIdx = (leaderIdx + 1) % len(rc.Nodes)
			rc.Leader = rc.Nodes[leaderIdx]
		} else {
			if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
				//fmt.Printf("\tRESP:Request Successful.")
				return reply.Value, nil
			} else if reply.Code == ClientReplyCode_RETRY {
				fmt.Printf("\tRESP:Told to RETRY\n")
				time.Sleep(time.Millisecond * 500)
				tryCnt++
				if tryCnt > 5 {
					return "", fmt.Errorf("Retry limit hit")
				}
			} else if reply.Code == ClientReplyCode_NOT_LEADER {
				fmt.Printf("\tRESP:Contacted NOT_LEADER.")
				leaderIdx = 0
				if reply.LeaderNode != nil {
					rc.Leader = *(reply.LeaderNode)
				}
			} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
				//fmt.Printf("RESP: Request Failed")
				return "", fmt.Errorf("Request Failed")
			} else if reply.Code == ClientReplyCode_INVALID_KEY {
				return "", fmt.Errorf("INVALID_KEY")
			}
		}
	}
}

func (rc *RaftClient) SendClientPutRequest(key string, value string) error {
	opr := Operation{Type: OpType_PUT, Key: key, Value: value}
	cmd := Command{ClientId: rc.ClientId, SeqNum: rc.SeqNum, Op: &opr}
	request := ClientRequestArgs{Cmd: &cmd}

	tryCnt := 0
	leaderIdx := 0
	rc.SeqNum++
	fmt.Printf("Sending PUT Request\n")
	for {
		fmt.Printf("\n\tTrying PUT Request to %v\n", rc.Leader)
		reply, err := rc.ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			fmt.Printf("\tRESP: Error sending RPC to %v. %v\n", rc.Leader, err)
			/*if leaderIdx < len(rc.Nodes) {
				leaderIdx++
				rc.Leader = rc.Nodes[leaderIdx]
			} else {
				return err
			}*/
			leaderIdx = (leaderIdx + 1) % len(rc.Nodes)
			rc.Leader = rc.Nodes[leaderIdx]
		} else {
			if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
				//fmt.Printf("Request SUCCESSFUL")
				return nil
			} else if reply.Code == ClientReplyCode_RETRY {
				fmt.Printf("Request RETRY")
				time.Sleep(time.Millisecond * 500)
				tryCnt++
				if tryCnt > 5 {
					return fmt.Errorf("Retry limit hit")
				}
			} else if reply.Code == ClientReplyCode_NOT_LEADER {
				fmt.Printf("Request NOT_LEADER\n")
				leaderIdx = 0
				if reply.LeaderNode != nil {
					rc.Leader = *(reply.LeaderNode)
				}
			} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
				fmt.Printf("Request REQUEST_FAILED\n")
				return fmt.Errorf("Request Failed")
			}
		}
	}
}
