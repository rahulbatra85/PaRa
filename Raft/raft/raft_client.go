package raft

import (
	"fmt"
	"time"
)

type RaftClient struct {
	ClientId int32
	Leader   NodeAddr
	SeqNum   uint64
}

func MakeRaftClient(leaderNode NodeAddr) *RaftClient {
	var rc RaftClient
	rc.Leader = leaderNode

	//Register Client
	request := ClientRegisterArgs{}

	tryCnt := 0
	done := false
	for !done && tryCnt < 5 {
		reply, err := ClientRegisterRPC(&rc.Leader, request)
		if err != nil {
			return nil
		}
		if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
			done = true
			rc.Leader = *(reply.LeaderNode)
			rc.ClientId = reply.ClientId
		} else if reply.Code == ClientReplyCode_NOT_LEADER {
			if reply.LeaderNode != nil {
				rc.Leader = *(reply.LeaderNode)
			}
		} else if reply.Code == ClientReplyCode_RETRY {
			time.Sleep(time.Millisecond * 500)
		} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
			done = true
			return nil
		}
		tryCnt++
	}

	return &rc
}

func (rc *RaftClient) SendClientGetRequest(key string) (string, error) {
	opr := Operation{Type: OpType_GET, Key: key, Value: ""}
	cmd := Command{ClientId: rc.ClientId, SeqNum: rc.SeqNum, Op: &opr}
	request := ClientRequestArgs{Cmd: &cmd}

	tryCnt := 0

	rc.SeqNum++
	for {
		reply, err := ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			return "", err
		}
		if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
			return reply.Value, nil
		} else if reply.Code == ClientReplyCode_RETRY {
			time.Sleep(time.Millisecond * 500)
			tryCnt++
			if tryCnt > 5 {
				return "", fmt.Errorf("Retry limit hit")
			}
		} else if reply.Code == ClientReplyCode_NOT_LEADER {
			if reply.LeaderNode != nil {
				rc.Leader = *(reply.LeaderNode)
			}
		} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
			return "", fmt.Errorf("Request Failed")
		}
	}
}

func (rc *RaftClient) SendClientPutRequest(key string, value string) error {
	opr := Operation{Type: OpType_PUT, Key: key, Value: value}
	cmd := Command{ClientId: rc.ClientId, SeqNum: rc.SeqNum, Op: &opr}
	request := ClientRequestArgs{Cmd: &cmd}

	tryCnt := 0

	rc.SeqNum++
	for {
		reply, err := ClientRequestRPC(&rc.Leader, request)
		if err != nil {
			return err
		}
		if reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
			return nil
		} else if reply.Code == ClientReplyCode_RETRY {
			time.Sleep(time.Millisecond * 500)
			tryCnt++
			if tryCnt > 5 {
				return fmt.Errorf("Retry limit hit")
			}
		} else if reply.Code == ClientReplyCode_NOT_LEADER {
			if reply.LeaderNode != nil {
				rc.Leader = *(reply.LeaderNode)
			}
		} else if reply.Code == ClientReplyCode_REQUEST_FAILED {
			return fmt.Errorf("Request Failed")
		}
	}
}
