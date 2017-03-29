package raft

import (
	"fmt"
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

func (kv *KVApp) ApplyOperation(cmd Command) (string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.Op.Type == OpType_GET {
		value, ok := kv.kvds[cmd.Op.Key]
		if !ok {
			return "", fmt.Errorf("Key not found")
		} else {
			return value, nil
		}
	} else if cmd.Op.Type == OpType_PUT {
		kv.kvds[cmd.Op.Key] = cmd.Op.Value
		return "", nil
	} else {
		return "", fmt.Errof("Invalid Command")
	}
}
