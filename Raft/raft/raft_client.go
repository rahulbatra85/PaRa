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
}

func MakeRaftClient(nodes []NodeAddr, config *RaftClientConfig, clientID int32) *RaftClient {
	var rc RaftClient
	rc.Leader = nodes[0]
	rc.Config = config
	rc.Nodes = nodes

	//Register Client
	request := ClientRegisterArgs{}
	ClientInitTracers()

	if clientID > 0 {
		rc.ClientId = clientID
		return &rc
	} else {
		rc.INF("Raft Client Created. Trying to register with %v", rc.Leader)
		tryCnt := 0
		done := false
		leaderIdx := 0
		for !done && tryCnt < 50 {
			rc.INF("Raft Client Created. Trying to register with %v, cnt=%d", rc.Leader, tryCnt)
			reply, err := ClientRegisterRPC(&rc.Leader, request)
			if err != nil {
				rc.INF("Registration Request Error. %v", err)
				if leaderIdx < len(rc.Nodes) {
					leaderIdx++
					rc.Leader = rc.Nodes[leaderIdx]
				} else {
					return nil
				}
			}
			if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {

				done = true
				rc.Leader = *(reply.LeaderNode)
				rc.ClientId = reply.ClientId
				rc.INF("Registration Successful. Client_ID=%d", rc.ClientId)
				return &rc
			} else if reply.Code == ClientReplyCode_NOT_LEADER {
				rc.INF("Registration NOT_LEADER")
				leaderIdx = 0
				if reply.LeaderNode != nil {
					rc.Leader = *(reply.LeaderNode)
				}
			} else if reply.Code == ClientReplyCode_RETRY {
				rc.INF("Registration RETRY")
				time.Sleep(time.Millisecond * 500)
			} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
				rc.INF("Registration REQUEST_FAILED")
				done = true
				return nil
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

	tryCnt := 0
	leaderIdx := 0
	rc.SeqNum++
	for {
		reply, err := ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			if leaderIdx < len(rc.Nodes) {
				leaderIdx++
				rc.Leader = rc.Nodes[leaderIdx]
			} else {
				return "", err
			}
		}
		if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
			rc.INF("Request SUCCESSFUL")
			return reply.Value, nil
		} else if reply.Code == ClientReplyCode_RETRY {
			rc.INF("Request RETRY")
			time.Sleep(time.Millisecond * 500)
			tryCnt++
			if tryCnt > 5 {
				return "", fmt.Errorf("Retry limit hit")
			}
		} else if reply.Code == ClientReplyCode_NOT_LEADER {
			rc.INF("Request NOT_LEADER")
			leaderIdx = 0
			if reply.LeaderNode != nil {
				rc.Leader = *(reply.LeaderNode)
			}
		} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
			rc.INF("Request REQUEST_FAILED")
			return "", fmt.Errorf("Request Failed")
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
	for {
		reply, err := ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			if leaderIdx < len(rc.Nodes) {
				leaderIdx++
				rc.Leader = rc.Nodes[leaderIdx]
			} else {
				return err
			}
		}
		if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
			rc.INF("Request SUCCESSFUL")
			return nil
		} else if reply.Code == ClientReplyCode_RETRY {
			rc.INF("Request RETRY")
			time.Sleep(time.Millisecond * 500)
			tryCnt++
			if tryCnt > 5 {
				return fmt.Errorf("Retry limit hit")
			}
		} else if reply.Code == ClientReplyCode_NOT_LEADER {
			rc.INF("Request NOT_LEADER")
			leaderIdx = 0
			if reply.LeaderNode != nil {
				rc.Leader = *(reply.LeaderNode)
			}
		} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
			rc.INF("Request REQUEST_FAILED")
			return fmt.Errorf("Request Failed")
		}
	}
}
