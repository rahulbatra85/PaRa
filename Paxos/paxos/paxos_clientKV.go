package paxos

import (
	"fmt"
)

type PaxosClientKV struct {
	Config   *PaxosClientKVConfig
	ClientId int
	SeqNum   int
	Replicas []NodeAddr
	Conns    map[NodeAddr]*connection
}

/*
type ClientRegisterReplyMsg struct {
	Reply *ClientRegisterReply
	Err   error
}*/

type ClientReplyMsg struct {
	Reply *ClientReply
	Err   error
}

func MakePaxosClientKV(config *PaxosClientKVConfig, cid int, replicas []NodeAddr) *PaxosClientKV {
	var kvc PaxosClientKV
	kvc.Config = config
	kvc.ClientId = cid
	kvc.Replicas = append(kvc.Replicas, replicas...)
	kvc.SeqNum = 0

	ClientInitTracers()
	kvc.Conns = make(map[NodeAddr]*connection)
	for _, node := range kvc.Replicas {
		kvc.Conns[node] = MakeConnection(&node)
	}

	return &kvc
}

func (kvc *PaxosClientKV) SendGETRequest(key string) (string, error) {

	kvc.SeqNum++
	op := Operation{Type: OpType_GET, Key: key, Value: ""}
	cmd := Command{ClientId: kvc.ClientId, SeqNum: kvc.SeqNum, Op: op}
	req := ClientRequestArgs{Cmd: cmd}

	//Client Request
	//Send to all replicas in separate goroutines
	ReplyMsgCh := make(chan ClientReplyMsg, len(kvc.Replicas))
	for _, n := range kvc.Replicas {
		go func(n NodeAddr, req ClientRequestArgs, msgCh chan ClientReplyMsg) {
			msg := ClientReplyMsg{}
			msg.Reply, msg.Err = kvc.ClientRequestRPC(&n, req)
			msgCh <- msg
		}(n, req, ReplyMsgCh)
	}
	kvc.DBG("GET Request Sent to all Replicas")
	cnt := 0
	var msg ClientReplyMsg
	for {
		msg = <-ReplyMsgCh
		cnt++
		if msg.Err == nil {
			if msg.Reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
				kvc.DBG("GET Request_SUCCESSFUL")
				return msg.Reply.Value, nil
			} else if msg.Reply.Code == ClientReplyCode_INVALID_KEY {
				kvc.DBG("GET Request_INVALID_KEY")
				return "", fmt.Errorf("INVALID_KEY")
			} else {
				kvc.DBG("GET Request_REQUEST_FAILED")
				return "", fmt.Errorf("REQUEST_FAILED")
			}
		} else if cnt == len(kvc.Replicas) {
			kvc.DBG("Got ERR from all replicas")
			return "", msg.Err
		}
	}
}

func (kvc *PaxosClientKV) SendPUTRequest(key string, value string) error {
	kvc.SeqNum++
	op := Operation{Type: OpType_PUT, Key: key, Value: value}
	cmd := Command{ClientId: kvc.ClientId, SeqNum: kvc.SeqNum, Op: op}
	req := ClientRequestArgs{Cmd: cmd}

	//Client Request
	//Send to all replicas in separate goroutines
	ReplyMsgCh := make(chan ClientReplyMsg, len(kvc.Replicas))
	for _, n := range kvc.Replicas {
		go func(n NodeAddr, req ClientRequestArgs, msgCh chan ClientReplyMsg) {
			msg := ClientReplyMsg{}
			msg.Reply, msg.Err = kvc.ClientRequestRPC(&n, req)
			msgCh <- msg
		}(n, req, ReplyMsgCh)
	}
	kvc.DBG("PUT Request Sent to all Replicas")
	cnt := 0
	var msg ClientReplyMsg
	for {
		msg = <-ReplyMsgCh
		cnt++
		if msg.Err == nil {
			if msg.Reply.Code == ClientReplyCode_REQUEST_SUCCESSFUL {
				kvc.DBG("PUT Request_SUCCESSFUL")
				return nil
			} else {
				kvc.DBG("PUT Request_REQUEST_FAILED")
				return fmt.Errorf("REQUEST_FAILED")
			}
		} else if cnt == len(kvc.Replicas) {
			kvc.DBG("Got ERR from all replicas")
			return msg.Err
		}
	}
}
