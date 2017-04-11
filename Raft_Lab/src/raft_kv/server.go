//Name: raft.go
//Description: Main file for raft server
//Author: Rahul Batra

package raft_kv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"os"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Fprintf(os.Stdout, format, a...)
	}
	return
}

type Op struct {
	Type     string
	Key      string
	Value    string
	ClientId int
	ReqId    int
}

type RaftKV struct {
	muKVDB         sync.RWMutex
	muNotifyCh     sync.RWMutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	kvdb           map[string]string
	chMap          map[int]chan ClientApply
	clientReplyMap map[int]ClientApply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVSERVER%d: Get Enter. %v", kv.me, args)
	cmd := Op{Type: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, ReqId: args.ReqId}

	//Check for duplicate request
	rep, found := kv.GetClientReplyMap(args.ClientId, args.ReqId)
	if found == true && args.ReqId == rep.ReqId {
		DPrintf("KVSERVER%d: Replayed Request %v\n", kv.me, args)
		reply.Err = rep.Err
		reply.Value = rep.Value
		reply.WrongLeader = 2
		//If this request was applied previously, but older than the last applied for this client
	} else if found == true && args.ReqId < rep.ReqId {
		reply.Err = OldRequest
	} else {
		idx, term, isLeader := kv.rf.Start(cmd)
		if isLeader == false {
			reply.WrongLeader = 1
			DPrintf("KVSERVER%d: WrongLeader %v\n", kv.me, args)
		} else {
			DPrintf("KVSERVER%d: Request %v sent to potential Leader @idx=%d\n", kv.me, args, idx)
			ch := make(chan ClientApply, 1)
			kv.PutNotifyCh(idx, ch)
			var msg ClientApply
			done := false
			timeout := false
			for !done {
				select {
				case msg = <-ch:
					done = true
					kv.RmNotifyCh(idx)
				case <-time.After(time.Duration(500) * time.Millisecond):
					termNow, isNowLeader := kv.rf.GetState()
					if isNowLeader == false || termNow != term {
						timeout = true
						kv.RmNotifyCh(idx)
						done = true
					}
				}
			}
			if timeout == false {
				termNow, isNowLeader := kv.rf.GetState()
				//Make sure this server is still leader
				DPrintf("KVSERVER%d: Heard on  notifyCh args=%v,reply=%v\n", kv.me, args, reply)
				if isNowLeader == false || termNow != term {
					DPrintf("KVSERVER%d: Not leader anymore Isleader=%v, termNow=%d, term=%d\n", kv.me, isNowLeader, termNow, term)
					reply.WrongLeader = 1
				} else {
					if msg.ClientId == args.ClientId && msg.ReqId >= args.ReqId {
						reply.Value = msg.Value
						reply.Err = msg.Err
						reply.WrongLeader = 2
						DPrintf("KVSERVER[%d]: Success with idx=%d for req=%v\n", kv.me, idx, args)
					}
				}
			} else {
				DPrintf("KVSERVER%d: Timeout and not leader anymore \n", kv.me)
				reply.WrongLeader = 1
			}
		}
	}
	DPrintf("KVSERVER: GET Exit. %v\n", reply)

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("KVSERVER%d: PUT Enter. %v\n", kv.me, args)
	cmd := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, ReqId: args.ReqId}

	rep, found := kv.GetClientReplyMap(args.ClientId, args.ReqId)
	if found == true && args.ReqId == rep.ReqId {
		reply.Err = rep.Err
		reply.WrongLeader = 2
		DPrintf("KVSERVER%d: Replayed Request %v\n", kv.me, args)
	} else if found == true && args.ReqId < rep.ReqId {
		reply.Err = OldRequest
	} else {
		idx, term, isLeader := kv.rf.Start(cmd)
		if isLeader == false {
			reply.WrongLeader = 1
			DPrintf("KVSERVER%d: WrongLeader %v\n", kv.me, args)
		} else {
			DPrintf("KVSERVER%d: Request %v sent to potential Leader @idx=%d\n", kv.me, args, idx)
			ch := make(chan ClientApply, 1)
			kv.PutNotifyCh(idx, ch)
			var msg ClientApply
			done := false
			timeout := false
			for !done {
				select {
				case msg = <-ch:
					done = true
					kv.RmNotifyCh(idx)
					DPrintf("KVSERVER%d: Heard on notifyCh args=%v, reply=%v\n", kv.me, args, msg)
				case <-time.After(time.Duration(500) * time.Millisecond):
					termNow, isNowLeader := kv.rf.GetState()
					if isNowLeader == false || termNow != term {
						timeout = true
						kv.RmNotifyCh(idx)
						done = true
						DPrintf("KVSERVER%d: Timeout, and not leader anymore \n", kv.me)
					}
				}
			}
			if timeout == false {
				termNow, isNowLeader := kv.rf.GetState()
				if isNowLeader == false || termNow != term {
					DPrintf("KVSERVER%d: Not leader anymore Isleader=%v, termNow=%d, term=%d\n", kv.me, isNowLeader, termNow, term)
					reply.WrongLeader = 1
				} else {
					if msg.ClientId == args.ClientId && msg.ReqId >= args.ReqId {
						reply.Err = msg.Err
						reply.WrongLeader = 2
						DPrintf("KVSERVER%d: Success with idx=%d for req=%v\n", kv.me, idx, args)
					}
				}
			} else {
				reply.WrongLeader = 1
			}
		}
	}
	DPrintf("KVSERVER%d: PUT Exit. %v\n", kv.me, reply)

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

	kv.kvdb = make(map[string]string)
	kv.chMap = make(map[int]chan ClientApply)
	kv.clientReplyMap = make(map[int]ClientApply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ProcessApplyCh()
	return kv
}

func (kv *RaftKV) ProcessApplyCh() {
	for {
		amsg := <-kv.applyCh
		msg := amsg.Command.(Op)

		//Should we apply this entry?
		cApply := ClientApply{}
		found := false
		cApply, found = kv.GetClientReplyMap(msg.ClientId, msg.ReqId)
		if found == false || (found == true && msg.ReqId > cApply.ReqId) {
			if msg.Type == "Put" {
				kv.PutKVDB(msg.Key, msg.Value)
			} else if msg.Type == "Append" {
				kv.AppendKVDB(msg.Key, msg.Value)
			} else if msg.Type == "Get" {
				value, err := kv.GetKVDB(msg.Key)
				cApply.Err = err
				cApply.Value = value
			}
			cApply.ClientId = msg.ClientId
			cApply.ReqId = msg.ReqId
			kv.PutClientReplyMap(msg.ClientId, cApply)
			DPrintf("KVSERVER%d: idx=%d with cmd=%v applied\n", kv.me, amsg.Index, msg)
		} else {
			DPrintf("KVSERVER%d: idx=%d with cmd=%v NOT applied\n", kv.me, amsg.Index, msg)
		}

		//Notify the Get or PutAppend handler to relay this result back to the client
		ch := kv.GetNotifyCh(amsg.Index)
		if ch != nil {
			DPrintf("KVSERVER%d Heard on Apply Ch %v. Forwarding to %d\n", kv.me, amsg, amsg.Index)
			ch <- cApply
		} else {
			DPrintf("KVSERVER%d Heard on Apply Ch %v, but no one registered\n", kv.me, amsg)
		}
	}
}

func (kv *RaftKV) GetKVDB(key string) (string, Err) {
	val, ok := kv.kvdb[key]
	if ok {
		return val, "OK"
	} else {
		return "", "ErrNoKey"
	}
}

func (kv *RaftKV) PutKVDB(key string, value string) {
	kv.kvdb[key] = value
}

func (kv *RaftKV) AppendKVDB(key string, value string) {
	kv.kvdb[key] = kv.kvdb[key] + value
}

func (kv *RaftKV) GetNotifyCh(idx int) chan ClientApply {
	kv.muNotifyCh.RLock()
	defer kv.muNotifyCh.RUnlock()
	ch, ok := kv.chMap[idx]
	if ok {
		return ch
	} else {
		return nil
	}
}

func (kv *RaftKV) PutNotifyCh(idx int, ch chan ClientApply) {
	kv.muNotifyCh.Lock()
	defer kv.muNotifyCh.Unlock()
	kv.chMap[idx] = ch
}

func (kv *RaftKV) RmNotifyCh(idx int) {
	kv.muNotifyCh.Lock()
	defer kv.muNotifyCh.Unlock()
	delete(kv.chMap, idx)
}

func (kv *RaftKV) GetClientReplyMap(id int, ReqId int) (ClientApply, bool) {
	reply, ok := kv.clientReplyMap[id]
	if ok {
		return reply, true
	} else {
		return reply, false
	}
}

func (kv *RaftKV) PutClientReplyMap(id int, reply ClientApply) {
	kv.clientReplyMap[id] = reply
}
