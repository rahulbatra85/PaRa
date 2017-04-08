//Name: server.go
//Description: Implements Key-Value Server and contains a local Paxos peer
//Author: Rahul Batra

package paxos_kv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string
	Key      string
	Value    string
	KVServer int
	ReqID    int
	ClientID int
}
type ApplyMsg struct {
	Seq     int
	Command interface{}
	Status  paxos.Fate
}

type PaxosKVServer struct {
	muKVDB     sync.RWMutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	kvdb           map[string]string //Key Value Database
	clientReplyMap map[int]ClientApplied
}

func (kv *PaxosKVServer) Get(args *GetArgs, reply *GetReply) error {
	DPrintf("KVSERVER[%d]: Get Enter. %v\n", kv.me, *args)
	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()

	//Parse Args and create Op
	logEntry := Op{}
	logEntry.Type = "Get"
	logEntry.Key = args.Key
	logEntry.KVServer = kv.me
	logEntry.ReqID = args.ReqID
	logEntry.ClientID = args.ClientID

	//If this request was last applied for this client
	rep, found := kv.GetClientReplyMap(args.ClientID, args.ReqID)
	if found == true && args.ReqID == rep.ReqID {
		DPrintf("KVSERVER[%d]: Replayed Request %v\n", kv.me, args)
		reply.Err = rep.Err
		reply.Value = rep.Value
		return nil
		//If this request was applied previously, but old than the last applied for this client
	} else if found == true && args.ReqID < rep.ReqID {
		DPrintf("KVSERVER[%d]: Old Request. Ignoring %v\n", kv.me, args)
		return fmt.Errorf("Old request")
	} else {
		done := false
		for !done {
			//Let's try to find a slot for this in the log
			seq := kv.px.Max() + 1
			kv.px.Start(seq, logEntry)
			DPrintf("KVSERVER[%d]: Trying GET on seq=%d\n", kv.me, seq)

			//Wait until this sequence has been decided
			status, v := kv.waitPxInstance(seq)
			if status == paxos.Decided {
				DPrintf("KVSERVER[%d]: Seq=%d Decided\n", kv.me, seq)
				r := v.(Op)

				//Should we apply this seq?
				cApply := ClientApplied{}
				found := false
				cApply, found = kv.GetClientReplyMap(r.ClientID, r.ReqID)
				if found == false || (found == true && r.ReqID > cApply.ReqID) {
					cApply.ReqID = r.ReqID
					cApply.Err = OK
					cApply.Value = ""
					if r.Type == "Put" {
						kv.PutKVDB(r.Key, r.Value)
					} else if r.Type == "Append" {
						kv.AppendKVDB(r.Key, r.Value)
					} else if r.Type == "Get" {
						value, err := kv.GetKVDB(r.Key)
						cApply.Err = err
						cApply.Value = value
					}
					kv.PutClientReplyMap(r.ClientID, cApply)
					DPrintf("KVSERVER[%d]: Seq=%d with args=%v applied", kv.me, seq, r)
				}

				//Do we respond to this client request? or try again?
				if r.ClientID == args.ClientID && r.ReqID >= args.ReqID {
					//done
					reply.Err = cApply.Err
					reply.Value = cApply.Value
					kv.px.Done(seq)
					done = true
					DPrintf("KVSERVER[%d]: Success with seq=%d for req=%v\n", kv.me, seq, args)
				}
			} else {
				DPrintf("KVSERVER[%d]: Seq=%d already forgotten!\n", kv.me, seq)
			}
		}
	}
	DPrintf("KVSERVER[%d]: GET Exit. %v\n", kv.me, args)
	return nil
}

func (kv *PaxosKVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	DPrintf("KVSERVER[%d]: PUT Enter %v\n", kv.me, *args)

	kv.muKVDB.Lock()
	defer kv.muKVDB.Unlock()

	logEntry := Op{}
	logEntry.Type = args.Op
	logEntry.Key = args.Key
	logEntry.Value = args.Value
	logEntry.KVServer = kv.me
	logEntry.ReqID = args.ReqID
	logEntry.ClientID = args.ClientID

	//If this request was last applied for this client
	rep, found := kv.GetClientReplyMap(args.ClientID, args.ReqID)
	if found == true && args.ReqID == rep.ReqID {
		DPrintf("KVSERVER[%d]: Replayed Request %v\n", kv.me, args)
		reply.Err = rep.Err
		return nil
		//If this request was applied previously, but older than the last applied for this client
	} else if found == true && args.ReqID < rep.ReqID {
		return fmt.Errorf("Old Request")
		DPrintf("KVSERVER[%d]: Old Request. Ignoring %v\n", kv.me, args)
	} else {
		done := false
		for !done {
			//Let's try to find a slot for this in the log
			seq := kv.px.Max() + 1
			kv.px.Start(seq, logEntry)
			DPrintf("KVSERVER[%d]: Trying GET on seq=%d\n", kv.me, seq)

			//Wait until this sequence has been decided
			status, v := kv.waitPxInstance(seq)
			if status == paxos.Decided {
				DPrintf("KVSERVER[%d]: Seq=%d Decided\n", kv.me, seq)
				r := v.(Op)

				//Should we apply this seq?
				cApply := ClientApplied{}
				found := false
				cApply, found = kv.GetClientReplyMap(r.ClientID, r.ReqID)
				if found == false || (found == true && r.ReqID > cApply.ReqID) {
					cApply.ReqID = r.ReqID
					cApply.Err = OK
					cApply.Value = ""
					if r.Type == "Put" {
						kv.PutKVDB(r.Key, r.Value)
					} else if r.Type == "Append" {
						kv.AppendKVDB(r.Key, r.Value)
					} else if r.Type == "Get" {
						value, err := kv.GetKVDB(r.Key)
						cApply.Err = err
						cApply.Value = value
					}
					kv.PutClientReplyMap(r.ClientID, cApply)
					DPrintf("KVSERVER[%d]: Seq=%d with args=%v applied", kv.me, seq, r)
				}

				//Do we respond to this client request? or try again?
				if r.ClientID == args.ClientID && r.ReqID >= args.ReqID {
					reply.Err = cApply.Err
					kv.px.Done(seq)
					done = true
					DPrintf("KVSERVER[%d]: Success with seq=%d for req=%v\n", kv.me, seq, args)
				}
			} else {
				DPrintf("KVSERVER[%d]: Seq=%d already forgotten!\n", kv.me, seq)
			}
		}
	}

	DPrintf("KVSERVER[%d]: PUT Exit. %v\n", kv.me, *args)
	return nil
}

// tell the server to shut itself down.
func (kv *PaxosKVServer) Kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *PaxosKVServer) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

//For testing
func (kv *PaxosKVServer) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *PaxosKVServer) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *PaxosKVServer {
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(PaxosKVServer)
	kv.me = me

	kv.kvdb = make(map[string]string)
	kv.clientReplyMap = make(map[int]ClientApplied)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("PaxosKVServer(%v) accept: %v\n", me, err.Error())
				kv.Kill()
			}
		}
	}()

	return kv
}

func (kv *PaxosKVServer) GetKVDB(key string) (string, Err) {
	DPrintf("KVSERVER[%d]: GetKVDB %s\n", kv.me, key)
	val, ok := kv.kvdb[key]
	if ok {
		return val, "OK"
	} else {
		return "", "ErrNoKey"
	}
}

func (kv *PaxosKVServer) PutKVDB(key string, value string) {
	DPrintf("KVSERVER[%d]: PutKVDB %s %s\n", kv.me, key, value)
	kv.kvdb[key] = value
}

func (kv *PaxosKVServer) AppendKVDB(key string, value string) {
	DPrintf("KVSERVER[%d]: AppendKVDB %s %s\n", kv.me, key, value)
	kv.kvdb[key] = kv.kvdb[key] + value
}

func (kv *PaxosKVServer) GetClientReplyMap(id int, ReqId int) (ClientApplied, bool) {
	reply, ok := kv.clientReplyMap[id]
	if ok {
		return reply, true
	} else {
		DPrintf("KVSERVER[%d]: GetClientReply not found Id=%d ReqId=%d\n", kv.me, id, ReqId)
		return reply, false
	}
}

func (kv *PaxosKVServer) PutClientReplyMap(id int, reply ClientApplied) {
	DPrintf("KVSERVER[%d]: PutClientReply Id=%d ReqId=%d\n", kv.me, id, reply.ReqID)
	kv.clientReplyMap[id] = reply
}

func (kv *PaxosKVServer) waitPxInstance(seq int) (paxos.Fate, interface{}) {
	to := 10 * time.Millisecond
	for {
		status, op := kv.px.Status(seq)
		if status == paxos.Pending {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		} else {
			return status, op
		}
	}
}
