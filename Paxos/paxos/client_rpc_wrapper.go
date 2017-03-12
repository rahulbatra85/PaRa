package paxos

import (
	"net/rpc"
)

type ClientRPCServer struct {
	client *KVClient
}

func (c *ClientRPCServer) startClientRPCServer() {

	for {

		conn, err := c.client.listener.Accept()
		if err != nil {
			continue
		}

		go rpc.ServeConn(conn)
	}
}

//Client Response Wrapper
func (c *ClientRPCServer) ResponseWrapper(req *ResponseRequest, reply *ReponseReply) error {
	err := c.client.Response(req)

	reply.Success = err == nil
	return err
}
