package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"os"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)
		fmt.Fprintf(os.Stdout, format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int
	ReqId    int
}

type RaftKV struct {
	muKVDB     sync.RWMutex
	muNotifyCh sync.RWMutex

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	//	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb           map[string]string
	chMap          map[int]chan raft.ApplyMsg
	clientReplyMap map[int]ClientReply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{Type: "GET", Key: args.Key, Value: "", ClientId: args.ClientId, ReqId: args.ReqId}
	DPrintf("KVSERVER: Get Enter. %v", args)
	//Check for duplicate request
	rep, found := kv.GetClientReplyMap(args.ClientId, args.ReqId)
	if found == true {
		reply.Err = rep.Err
		reply.Value = rep.Value
		reply.WrongLeader = 2
		DPrintf("KVSERVER: Replayed Request %v\n", args)
	} else {
		idx, term, isLeader := kv.rf.Start(cmd)
		if isLeader == false {
			reply.WrongLeader = 1
			DPrintf("KVSERVER: WrongLeader %v\n", args)
		} else {
			ch := make(chan raft.ApplyMsg)
			kv.PutNotifyCh(idx, ch)
			msg := <-ch
			kv.RmNotifyCh(idx)
			termNow, isNowLeader := kv.rf.GetState()
			//Make sure this server is still leader
			DPrintf("KVSERVER: Heard from applyCh %v\n", args)
			if isNowLeader == false || termNow != term {
				DPrintf("KVSERVER: Not leader anymore Isleader=%v, termNow=%d, term=%d\n", isNowLeader, termNow, term)
				reply.WrongLeader = 1
			} else {
				DPrintf("KVSERVER: Success!\n")
				val, err := kv.GetKVDB(msg.Command.(Op).Key)
				reply.Value = val
				reply.Err = err
				reply.WrongLeader = 2
				cReply := ClientReply{ReqId: args.ReqId, WrongLeader: 2, Err: reply.Err, Value: reply.Value}
				kv.PutClientReplyMap(args.ClientId, cReply)
			}
		}
	}
	DPrintf("KVSERVER: GET Exit. %v\n", reply)

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("KVSERVER: PUT Enter. %v", args)
	cmd := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, ReqId: args.ReqId}
	rep, found := kv.GetClientReplyMap(args.ClientId, args.ReqId)
	if found == true {
		reply.Err = rep.Err
		reply.WrongLeader = 2
		DPrintf("KVSERVER: Replayed Request %v", args)
	} else {
		idx, term, isLeader := kv.rf.Start(cmd)
		if isLeader == false {
			reply.WrongLeader = 1
			DPrintf("KVSERVER: WrongLeader %v\n", args)
		} else {
			ch := make(chan raft.ApplyMsg)
			kv.PutNotifyCh(idx, ch)
			msg := <-ch
			kv.RmNotifyCh(idx)
			termNow, isNowLeader := kv.rf.GetState()
			DPrintf("KVSERVER: Heard from applyCh %v\n", args)
			if isNowLeader == false || termNow != term {
				DPrintf("KVSERVER: Not leader anymore Isleader=%v, termNow=%d, term=%d\n", isNowLeader, termNow, term)
				reply.WrongLeader = 1
			} else {
				DPrintf("KVSERVER: Success!\n")
				reply.Err = "OK"
				reply.WrongLeader = 2
				if args.Op == "Put" {
					kv.PutKVDB(msg.Command.(Op).Key, msg.Command.(Op).Value)
				} else {
					kv.AppendKVDB(msg.Command.(Op).Key, msg.Command.(Op).Value)
				}
				cReply := ClientReply{ReqId: args.ReqId, WrongLeader: 2, Err: reply.Err, Value: ""}
				kv.PutClientReplyMap(args.ClientId, cReply)
			}
		}
	}
	DPrintf("KVSERVER: PUT Exit. %v\n", reply)

	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	//	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.kvdb = make(map[string]string)
	kv.chMap = make(map[int]chan raft.ApplyMsg)
	kv.clientReplyMap = make(map[int]ClientReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ProcessApplyCh()
	return kv
}

func (kv *RaftKV) ProcessApplyCh() {
	for {
		msg := <-kv.applyCh
		ch := kv.GetNotifyCh(msg.Index)
		if ch != nil {
			ch <- msg
		}
	}
}

func (kv *RaftKV) GetKVDB(key string) (string, Err) {
	kv.muKVDB.RLock()
	defer kv.muKVDB.RUnlock()
	val, ok := kv.kvdb[key]
	if ok {
		return val, "OK"
	} else {
		return "", "ErrNoKey"
	}
}

func (kv *RaftKV) PutKVDB(key string, value string) {
	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()
	kv.kvdb[key] = value
}

func (kv *RaftKV) AppendKVDB(key string, value string) {
	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()
	kv.kvdb[key] = kv.kvdb[key] + value
}

func (kv *RaftKV) GetNotifyCh(idx int) chan raft.ApplyMsg {
	kv.muNotifyCh.RLock()
	defer kv.muNotifyCh.RUnlock()
	ch, ok := kv.chMap[idx]
	if ok {
		return ch
	} else {
		return nil
	}

}

func (kv *RaftKV) PutNotifyCh(idx int, ch chan raft.ApplyMsg) {
	kv.muNotifyCh.Lock()
	defer kv.muNotifyCh.Unlock()
	kv.chMap[idx] = ch
}

func (kv *RaftKV) RmNotifyCh(idx int) {
	kv.muNotifyCh.Lock()
	defer kv.muNotifyCh.Unlock()
	delete(kv.chMap, idx)
}

func (kv *RaftKV) GetClientReplyMap(id int, ReqId int) (ClientReply, bool) {
	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()
	reply, ok := kv.clientReplyMap[id]
	if ok {
		if reply.ReqId == ReqId {
			return reply, true
		} else {
			return reply, false
		}
	} else {
		return reply, false
	}
}

func (kv *RaftKV) PutClientReplyMap(id int, reply ClientReply) {
	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()
	kv.clientReplyMap[id] = reply
}
