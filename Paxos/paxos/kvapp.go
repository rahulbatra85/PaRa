package paxos

import (
	"sync"
)

//Key-Value Datastore Application

type KVApp struct {
	kvds map[string]string //Key-Value datastore
	mu   sync.Mutex
}

func MakeKVApp() *KVApp {
	var kv KVApp
	kv.kvds = make(map[string]string)

	return &kv
}

func (kv *KVApp) ApplyOperation(cmd Command) (string, ClientReplyCode) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.Op.Type == OpType_GET {
		value, ok := kv.kvds[cmd.Op.Key]
		if !ok {
			return "", ClientReplyCode_INVALID_KEY
		} else {
			return value, ClientReplyCode_REQUEST_SUCCESSFUL
		}
	} else if cmd.Op.Type == OpType_PUT {
		kv.kvds[cmd.Op.Key] = cmd.Op.Value
		return "", ClientReplyCode_REQUEST_SUCCESSFUL
	} else {
		return "", ClientReplyCode_INVALID_COMMAND
	}
}
