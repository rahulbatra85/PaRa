package paxos

type KVApplication struct {
}

func CreateApplication() *KVApplication {
	var kv KVApplication

	return &kv
}

func (kv *KVApplication) ApplyOperation(op Operation) {
}
