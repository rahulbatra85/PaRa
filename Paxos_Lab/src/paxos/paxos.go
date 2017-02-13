package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "math"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
	MaxInt    = int(^uint(0) >> 1)
)

type Paxos struct {
	mu         sync.RWMutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// data.
	logNumPeers   int
	pxInstances   map[int]pxInstance
	pxAcInstances map[int]pxAcceptorInstance
	doneValues    map[string]int
	maxSeqSeen    int
	deletedSeq    int
}

type pxInstance struct {
	status Fate
	v      interface{}
}

type pxAcceptorInstance struct {
	seq int
	n   int
	np  int
	na  int
	va  interface{}
}

//Prepare RPC arguments structure
type PrepareArgs struct {
	Seq int
	N   int
}

//Prepare RPC response structure
type PrepareRespArgs struct {
	Seq  int
	OK   bool //If false, then reject
	N    int
	Na   int
	Va   interface{}
	Done int
}

//Accept RPC arguments structure
type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

//Accept RPC response structure
type AcceptRespArgs struct {
	Seq int
	OK  bool
	N   int
}

//Decide RPC arguments structure
type DecidedArgs struct {
	Seq int
	V   interface{}
}

//Decide RPC Response structure
type DecidedRespArgs struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// call() will time out and return an error after a while
//if it does not get a reply from the server.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//Runs in a go routine which is called by Start
//Start is called by application requesting to replicate an entry
func (px *Paxos) propose(seq int, v interface{}) {
	px.mu.Lock()
	pxi := px.pxInstances[seq]
	px.mu.Unlock()

	pxme := px.peers[px.me]
	thisDecided := pxi.status

	//////////////////////////////////////////////
	//Phase 1
	//////////////////////////////////////////////
	type prepItem struct {
		reply *PrepareRespArgs
		ok    bool
	}
	//while not decided:
	m := 1
	//fmt.Printf(" %s %d %v Phase 1\n", pxme, seq, v)
	for thisDecided != Decided && px.isdead() != true {
		//choose n, unique and higher than any n seen so far
		n := (px.me + 1) * m //Initial value of n

		//send prepare(n) to all servers including self
		replyMap := make(map[string]prepItem)
		for _, p := range px.peers {
			if p != pxme {
				//			go func(p string, n int, seq int) {
				args := &PrepareArgs{}
				args.Seq = seq
				args.N = n
				pci := prepItem{}
				pci.reply = &PrepareRespArgs{}
				pci.ok = call(p, "Paxos.Prepare", args, pci.reply)
				replyMap[p] = pci
			} else {
				args := &PrepareArgs{}
				args.N = n
				args.Seq = seq
				pci := prepItem{}
				pci.reply = &PrepareRespArgs{}
				px.Prepare(args, pci.reply)
				pci.ok = true
				replyMap[p] = pci
			}
		}

		//Gather responses
		nok := 0
		maxNa := 0
		var maxVa interface{}
		maxVa = nil
		//fmt.Printf("%v %d %v PrepareResponses Gathering\n", pxme, seq, v)
		px.mu.Lock()
		for p, pci := range replyMap {

			//pci := <-pChan
			if pci.ok {
				if pci.reply.OK {
					nok = nok + 1
					if pci.reply.Na > maxNa {
						maxNa = pci.reply.Na
						maxVa = pci.reply.Va
					}
				}
				px.doneValues[p] = pci.reply.Done
			}
		}
		px.mu.Unlock()

		//////////////////////////////////////////////
		//Phase 2
		//////////////////////////////////////////////
		//If prepareOK from majority:
		if nok >= len(px.peers)/2+1 {
			//fmt.Printf(" %s %d %v Phase 2 Majority \n", pxme, seq, v)
			//Choose v = va with highest na; Othwerwise, choose own v
			if maxNa != 0 {
				v = maxVa
			}
			//send Accept(n, v) to all
			type acItem struct {
				reply *AcceptRespArgs
				ok    bool
			}
			replyMap := make(map[string]acItem)
			for _, p := range px.peers {
				if p != pxme {
					var aci acItem
					args := &AcceptArgs{}
					args.Seq = seq
					args.N = n
					args.V = v
					aci.reply = &AcceptRespArgs{}
					aci.ok = call(p, "Paxos.Accept", args, aci.reply)
					replyMap[p] = aci
				} else {
					var aci acItem
					args := &AcceptArgs{}
					args.Seq = seq
					args.N = n
					args.V = v
					aci.reply = &AcceptRespArgs{}
					px.Accept(args, aci.reply)
					aci.ok = true
					replyMap[p] = aci
				}
			}

			//Gather responses
			//fmt.Printf("%v %d %v AcceptResponses Gathering\n", pxme, seq, v)
			nok := 0
			for _, ac := range replyMap {
				if ac.ok && ac.reply.OK {
					nok = nok + 1
				}
			}

			//////////////////////////////////////////////
			//Phase 3
			//////////////////////////////////////////////
			//if AcceptOK(n) from majority:
			//send Decided(v) to all
			if nok >= len(px.peers)/2+1 {
				//fmt.Printf(" %s %d %v Phase 3 Majority \n", pxme, seq, v)
				for _, p := range px.peers {
					if p != pxme {
						args := &DecidedArgs{}
						args.Seq = seq
						args.V = v
						var reply DecidedRespArgs
						call(p, "Paxos.Decide", args, &reply)
					} else {
						px.mu.Lock()
						var pi pxInstance
						pi.status = Decided
						pi.v = v
						px.pxInstances[seq] = pi
						px.mu.Unlock()
						thisDecided = Decided
						//	fmt.Printf(" %s %d %v Decided\n", pxme, seq, v)
					}
				}
			}
		}

		m = m + 1
	}
	//	fmt.Printf(" %s %d %v Exit\n", pxme, seq, v)
}

//
//Prepare RPC Handler
//
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareRespArgs) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	pxi := px.pxAcInstances[args.Seq]
	if args.N > pxi.np {
		pxi.np = args.N
		px.pxAcInstances[args.Seq] = pxi
		reply.Seq = args.Seq
		reply.N = args.N
		reply.Na = pxi.na
		reply.Va = pxi.va
		reply.OK = true
		reply.Done = px.doneValues[px.peers[px.me]]
	} else {
		reply.Seq = pxi.seq
		reply.OK = false
		reply.Done = px.doneValues[px.peers[px.me]]
	}

	return nil
}

//
//Accept RPC Handler
//
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptRespArgs) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	pxai := px.pxAcInstances[args.Seq]
	if args.N >= pxai.np {
		pxai.np = args.N
		pxai.na = args.N
		pxai.va = args.V
		px.pxAcInstances[args.Seq] = pxai
		reply.Seq = args.Seq
		reply.OK = true
		reply.N = args.N
	} else {
		reply.Seq = args.Seq
		reply.OK = false
	}

	return nil
}

//
//Decide RPC Handler
//
func (px *Paxos) Decide(args *DecidedArgs, reply *DecidedRespArgs) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	var pxi pxInstance
	pxi.v = args.V
	pxi.status = Decided
	px.pxInstances[args.Seq] = pxi

	return nil
}

//
// The application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//kick off runPaxos in another routine
	px.mu.Lock()
	if seq > px.maxSeqSeen {
		px.maxSeqSeen = seq
	}
	pxi, ok := px.pxInstances[seq]
	if ok == false {
		pxi = pxInstance{}
		pxi.status = Pending
		px.pxInstances[seq] = pxi
		go px.propose(seq, v)
	}

	px.mu.Unlock()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.doneValues[px.peers[px.me]] = seq

	min := MaxInt
	for _, val := range px.doneValues {
		if val < min {
			min = val
		}
	}

	for s := px.deletedSeq + 1; s <= min+1; s++ {
		delete(px.pxInstances, s)
		delete(px.pxAcInstances, s)
	}
	px.deletedSeq = min
	px.mu.Unlock()
}

//
// The application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.maxSeqSeen
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	min := MaxInt
	for _, val := range px.doneValues {
		if val < min {
			min = val
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	var status Fate
	var v interface{}
	min := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < min {
		status = Forgotten
	} else {
		pxi := px.pxInstances[seq]
		status = pxi.status
		v = pxi.v
	}
	return status, v
}

// tell the peer to shut itself down.
// for testing.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

//Used for testing
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

//Used for testing
func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.logNumPeers = int(math.Log2(float64(len(px.peers))))
	px.pxInstances = make(map[int]pxInstance)
	px.pxAcInstances = make(map[int]pxAcceptorInstance)
	px.doneValues = make(map[string]int)
	for _, p := range px.peers {
		px.doneValues[p] = -1
	}
	px.maxSeqSeen = -1
	px.deletedSeq = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
