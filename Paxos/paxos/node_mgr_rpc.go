package paxos

import (
	"fmt"
	"net/rpc"
)

var mgrConn *rpc.Client

/////////////////////////////////////////
//ReadyRPC
/////////////////////////////////////////
type ReadyNotificationRequest struct {
	RemoteNode NodeAddr
	FromAddr   NodeAddr
}

type ReadyNotificationReply struct {
	Success bool
}

func ReadyNotificationRPC(remoteNode *NodeAddr, fromNode *NodeAddr) error {
	req := ReadyNotificationRequest{RemoteNode: *remoteNode, FromAddr: *fromNode}
	var reply ReadyNotificationReply
	err := makeNodeManagerRemoteCall(remoteNode, "ReadyNotificationWrapper", req, &reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Unable to notify")
	}

	return nil
}

func makeNodeManagerRemoteCall(remoteAddr *NodeAddr, procName string, request interface{}, reply interface{}) error {
	var err error
	if mgrConn == nil {
		mgrConn, err = rpc.Dial("tcp", remoteAddr.Addr)
		if err != nil {
			return err
		}
	}

	fullProcName := fmt.Sprintf("%v.%v", remoteAddr.Addr, procName)
	err = mgrConn.Call(fullProcName, request, reply)
	if err != nil {
		mgrConn = nil
	}
	return err
}
