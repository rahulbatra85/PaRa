package paxos

import (
	"sync"
)

//Key-Value Datastore Application

type KVApp struct {
	kvds map[string]string //Key-Value datastore
	mu   sync.Mutex
}

func CreateKVApp() *KVApp {
	var kv KVApp
	kv.kvds = make(map[string]string)

	return &kv
}

func (kv *KVApplication) ApplyOperation(op *Operation) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var ret string

	if op.Name == "GET" || op.Name == "get" {
		ret = kvds[op.Key]
	} else if op.Name == "PUT" || op.Name == "put" {
		ret = op.Data
	}

	return ret
}
