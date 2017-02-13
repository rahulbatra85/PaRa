package kvpaxos

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
import "strings"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Op       string
	Key      string
	Value    string
	KVServer int
	ReqID    int64
	ClientID string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	kvdb    map[string]string //Key Value Database
	seqUsed int
}

func (kv *KVPaxos) waitPxInstance(seq int) {
	decided := false
	to := 10 * time.Millisecond
	for !decided {
		status, _ := kv.px.Status(seq)
		if status != paxos.Decided {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		} else {
			decided = true
		}
	}
}

func (kv *KVPaxos) updateKVDB(r Op) {
	kv.kvdb[r.ClientID+":LastReqID"] = strconv.FormatInt(r.ReqID, 10)
	if r.Op == "Put" {
		kv.kvdb[r.Key] = r.Value
		kv.kvdb[r.ClientID+":LastReply"] = OK + ":" + ""
	} else if r.Op == "Append" {
		val, ok := kv.kvdb[r.Key]
		if ok {
			kv.kvdb[r.Key] = val + r.Value
		} else {
			kv.kvdb[r.Key] = r.Value
		}
		kv.kvdb[r.ClientID+":LastReply"] = OK + ":" + ""
	} else if r.Op == "Get" {
		//Set Reply Args
		val, ok := kv.kvdb[r.Key]
		if ok {
			kv.kvdb[r.ClientID+":LastReply"] = OK + ":" + val
		} else {
			kv.kvdb[r.ClientID+":LastReply"] = ErrNoKey + ":" + ""
		}
	}

	kv.seqUsed = kv.seqUsed + 1
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//Parse Args and create Op
	logEntry := Op{}
	logEntry.Op = "Get"
	logEntry.Key = args.Key
	logEntry.KVServer = kv.me
	logEntry.ReqID = args.ReqID
	logEntry.ClientID = args.ClientID
	//	fmt.Printf("%v Get %v\n", kv.me, logEntry)

	done := false
	for !done {
		if kv.kvdb[args.ClientID+":LastReqID"] == strconv.FormatInt(args.ReqID, 10) {
			done = true
			s := strings.Split(kv.kvdb[args.ClientID+":LastReply"], ":")
			if s[0] == OK {
				reply.Err = OK
				reply.Value = s[1]
			} else if s[0] == ErrNoKey {
				reply.Err = ErrNoKey
			}
		} else {
			//Increment Seq and call start
			seq := kv.seqUsed + 1
			kv.px.Start(seq, logEntry)

			//Wait for status to be decided
			kv.waitPxInstance(seq)

			var r Op
			_, v := kv.px.Status(seq)
			r = v.(Op)
			kv.updateKVDB(r)
		}
	}

	// Your code here.
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//Parse Args and create Op
	logEntry := Op{}
	logEntry.Op = args.Op
	logEntry.Key = args.Key
	logEntry.Value = args.Value
	logEntry.KVServer = kv.me
	logEntry.ReqID = args.ReqID
	logEntry.ClientID = args.ClientID
	//	fmt.Printf("%v PutAppend %v\n", kv.me, logEntry)

	done := false
	for !done {
		//Is this a duplicate request
		if kv.kvdb[args.ClientID+":LastReqID"] == strconv.FormatInt(args.ReqID, 10) {
			done = true
		} else {
			//Increment Seq and call start
			seq := kv.seqUsed + 1
			kv.px.Start(seq, logEntry)

			//Wait for status to be decided
			kv.waitPxInstance(seq)

			//Apply decided value
			var r Op
			_, v := kv.px.Status(seq)
			r = v.(Op)
			kv.updateKVDB(r)
		}
	}

	//Set Reply Args
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

//For testing
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kvdb = make(map[string]string)
	kv.seqUsed = -1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
