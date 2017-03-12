package paxos

import (
	"fmt"
)

func (c *KVClient) Response(request *ResponseRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if request.Cmd.SeqNum == c.SeqNum {
		fmt.Printf("Cmd=%s",
			request.Cmd.Cid,
			request.Cmd.SeqNum,
			request.Cmd.Op.Type,
			request.Cmd.Op.Key,
			request.Cmd.Op.Data)
		c.SeqNum++
	}

	return nil
}
