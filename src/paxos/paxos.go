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
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1 // 1
	Pending                   // 2 not yet decided
	Forgotten                 // 3 decided but forgotten
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32    // for testing
	unreliable int32    // for testing
	rpcCount   int32    // for testing
	peers      []string // list of ALL peers, including myself
	me         int      // index into peers[]

	recProposals map[int]*Proposal
	donePeers    map[int]int // same indexing as peers
}

type Proposal struct {
	Seq     int
	Value   interface{}
	Fate    Fate
	PropNum int
}

// the propose stage is also an interrogation stage
// I reply with the highest sequence and value pair I have seen
type InterrogationReply struct {
	Seq   int
	Value interface{}
	Num   int
	Error string
}

// on accept_ok, I return the sequence number I have accepted
type AcceptanceReply struct {
	Seq   int
	Error string
}

// no response needed from Decide
type DecideReply struct {
}

// put my highest done value
type DoneArgs struct {
	Me       int
	MaxValue int
}

// no response
type DoneReply struct {
}

////////

//
// Calculates the greater (whole) half of number of peers required for mjrty
//
func calculateMajority(peerCount int) int {
	temp := float64(peerCount) / 2.0
	return int(math.Ceil(temp))
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
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
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

//
// Acceptor function
// No response! The passed in value is FINAL
//
func (px *Paxos) Decide(args *Proposal, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if _, ok := px.recProposals[args.Seq]; !ok {
		// return a new empty proposal
		px.recProposals[args.Seq] = new(Proposal)
		px.recProposals[args.Seq].Fate = Pending
		px.recProposals[args.Seq].Seq = 0
		px.recProposals[args.Seq].PropNum = 0
		px.recProposals[args.Seq].Value = ""
	}

	px.recProposals[args.Seq] = args

	//  fmt.Println("Decided on..... ", args.Value) //, px.recProposals[args.Seq].Value)

	px.recProposals[args.Seq].Fate = Decided

	return nil
}

//
// Decide RPC handler
//
func (px *Paxos) HandleDecide(proposal *Proposal) {

	prop := proposal

	for me, peer := range px.peers {
		var decide_reply DecideReply

		if me == px.me {
			px.mu.Lock()
			// px.Decide(prop, &decide_reply)
			// px.recProposals[proposal.Seq] = &prop.Seq
			px.recProposals[proposal.Seq].Fate = Decided
			px.recProposals[proposal.Seq].Value = prop.Value
			//  fmt.Println("Decided on..... ", prop.Value)
			fmt.Println("\t", prop.Seq, prop.PropNum, "\t Decide --myself --: success! ", peer)
			px.mu.Unlock()
		} else {
			for i := 0; i < 5; i++ {
				if ok := call(peer, "Paxos.Decide", prop, &decide_reply); !ok {
					//fmt.Println("\t", prop.Seq, prop.PropNum, "\t Decide: call failed! ", peer)
					time.Sleep(10 * time.Millisecond)

				} else {
					// success; we should be all done
					fmt.Println("\t", prop.Seq, prop.PropNum, "\t Decide: success! ", peer)
					break
				}
			}
		}
	}
}

//
// Acceptor function
// Return accept_ok or accept_reject
// OK if sequence is GREATER OR EQUAL to what I've already seen
// else, I must reject
//
func (px *Paxos) Accept(args *Proposal, reply *AcceptanceReply) error {

	// atomically do this
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := args.Seq
	propnum := args.PropNum

	if _, ok := px.recProposals[args.Seq]; !ok {
		return nil
	}

	if propnum >= px.recProposals[seq].PropNum {
		px.recProposals[seq] = args

	} else {

		reply.Error = "proposed value is too small / out of date. Accept Rejected!"

	}

	return nil
}

//
// Accept RPC handler: Build the needed structs, etc
// Returns accept_ok or accept_reject
//
func (px *Paxos) HandleAccept(proposal *Proposal, N int, V interface{}) bool {

	prop := proposal

	Majority := calculateMajority(len(px.peers))

	// fmt.Println("Accept on: ", seq)

	ok_count := 0

	for me, peer := range px.peers {

		var accept_reply AcceptanceReply

		if me == px.me {
			response := px.Accept(prop, &accept_reply)

			if response == nil {
				// all is good
				ok_count++

				fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accepted: OK -- myself", peer)
			} else {
				fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accepted: rejected -- myself!", peer)
			}

		} else {
			for i := 0; i < 5; i++ {

				if ok := call(peer, "Paxos.Accept", prop, &accept_reply); !ok {
					//fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accept: Call failed! ", peer)

					time.Sleep(10 * time.Millisecond)

				} else if accept_reply.Error != "" {
					fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accept: rejected ", peer)
					break

				} else {
					// accept_ok success
					ok_count++

					fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accepted: OK", peer)
					break
				}
			}
		}
	}

	ok := ok_count >= Majority

	if ok {
		fmt.Println("\t", prop.Seq, prop.PropNum, "\t Accept: majority!")
	}

	return ok
}

//
// Acceptor function
// Return propose_ok or propose_reject
// Interrogation call;  Ok if sequence value is GREATER than what I've already seen,
//                      Else, I must reject.
func (px *Paxos) Propose(args *Proposal, reply *InterrogationReply) error {

	// atomically access
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := args.Seq
	propnum := args.PropNum

	_, key_exists := px.recProposals[seq]

	if !key_exists {
		px.recProposals[seq] = args

	} else {
		// fmt.Println("Propose: THIS SEQUENCE NUM EXISTS!")

		// first see if the other value has been decided
		if px.recProposals[seq].Fate == Decided {
			// fmt.Println("Propose: We already have a majority on this sequence number")
			// fmt.Println(seq, px.recProposals[seq].Value)

			// return interrogation: existing value and Seq
			reply.Seq = px.recProposals[seq].Seq
			reply.Value = px.recProposals[seq].Value
			reply.Num = px.recProposals[seq].PropNum

		} else {
			// fmt.Println("This sequence value is not accepted yet")

			// return interrogation as my proposed value
			reply.Seq = args.Seq
			reply.Value = args.Value
			reply.Num = propnum
		}

		if px.recProposals[seq].PropNum < propnum {
			px.recProposals[seq] = args

		} else {
			reply.Error = "Sequence value is too small / out of date. Rejected!"
		}

	}

	return nil
}

//
// Propose RPC handler: Build the needed structs, etc
// Send proposal to all peers, including myself
// Return either: this sequence ready to be accepted, or the seq in consensus
// Returns propose_ok or propose_reject
//
func (px *Paxos) HandlePropose(proposal *Proposal) (bool, int, interface{}) {
	prop := proposal

	ok_count := 0

	var replySeq int
	var replyValue interface{}

	Majority := calculateMajority(len(px.peers))

	fmt.Println("Proposal on: ", proposal.Seq)

	// send interrogation to ALL peers
	// replySeq and replyValue will be the majority decision
	for me, peer := range px.peers {

		var peer_reply InterrogationReply

		if me == px.me {

			response := px.Propose(prop, &peer_reply)

			if response == nil {
				// no error
				ok_count++

				// set reply
				// replySeq = peer_reply.Seq
				// replyValue = peer_reply.Value
				if peer_reply.Value != proposal.Value {

					// fmt.Println(peer_reply.Value, " self vs ", proposal.Value)

					if peer_reply.Value != nil {
						replySeq = peer_reply.Seq
						replyValue = peer_reply.Value
					}
				}

				fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: (self) All is good! ", peer)

			} else {
				fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: (self) Something went wrong! ", peer)
			}

		} else {

			// 5 retries if call fails
			for i := 0; i < 5; i++ {

				if ok := call(peer, "Paxos.Propose", prop, &peer_reply); !ok {
					time.Sleep(10 * time.Millisecond)

					//fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: Call Failed! ", peer)

				} else if peer_reply.Error != "" {
					// something is out of date -- REJECT & BREAK

					fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: My sequence is out of date", peer)
					break
				} else {
					// no error - SET & BREAK
					ok_count++

					// set reply
					// if it doesn't equal my proposed value, I know it
					// must have been DECIDED previously
					// what if I have two values, from two servers with
					// the same sequence? Tiebreaker == highest PropNum
					if peer_reply.Value != proposal.Value {
						if peer_reply.Value != nil {
							replySeq = peer_reply.Seq
							replyValue = peer_reply.Value
						}
					}

					fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: All is good", peer)
					break
				}
			}
		}
	}

	ok := ok_count >= Majority

	if ok {
		fmt.Println("\t", prop.Seq, prop.PropNum, "\t Propose: Majority")
	}

	return ok, replySeq, replyValue
}

//
// Generate number based on epoch nanseconds
//
func (px *Paxos) GenerateID(seq int) int {
	now := time.Now()
	nanos := now.UnixNano()
	ms := nanos / 1000000

	num := ms << 8

	if px.me > 255 {
		log.Fatal("px.me is too high (must be less than 256)")
	}

	s := int(num) + px.me

	return s
}

//
// Proposer function
// "Each instance should have it's own instance of the PAXOS protocol"
// i.e., implemented as a goroutine from Start()
// Each server has its own acceptance handling; PSEUDO CODE HERE!
//
func (px *Paxos) StartProtocol(seq int, v interface{}) {
	for {

		fmt.Printf("Trying to decide instance %d request from %d\n", seq, px.me)

		propose_ok := false
		accept_ok := false

		// build proposal
		proposal := new(Proposal)
		proposal.PropNum = px.GenerateID(seq)
		proposal.Seq = seq
		proposal.Value = v
		proposal.Fate = Pending

		fmt.Println("\t", seq, proposal.PropNum, "\tStart protocol!")

		propose_ok, N, V := px.HandlePropose(proposal)

		if propose_ok {
			// fmt.Println("\n\n\nCalling handle accept with...")
			// fmt.Println(N, V)
			// fmt.Println("\n\n\n")

			// ignore the proposal V, Seq?
			proposal.Value = V
			accept_ok = px.HandleAccept(proposal, N, V)
		}

		if accept_ok {
			px.HandleDecide(proposal)

		}

		// break once this seqence has been agreed on
		state, _ := px.Status(seq)
		if state == Decided {
			// fmt.Printf("We decided for %d on %d... state: %d == %d\n", seq, px.me, state, proposal.Fate)
			break
		}

		// fmt.Printf("BREATHE MAN!\n")
		//time.Sleep(100 * time.Millisecond)
	}
}

//
// the application wants paxos to start agreement/
// conensus on instance seq, with proposed value v.
// Start() returns IMMEDIATELY; the application will
// call Status() to find out if/when agreement
// is reached; starts concurrent PAXOS protocol
// Caveat: If Start() is called with a sequence number
// less than Min(), the Start() call should be ignored
//
func (px *Paxos) Start(seq int, v interface{}) {
	fmt.Println("Starting...", seq, px.Min())

	go func() {
		if seq < px.Min() {
			return
		}

		px.StartProtocol(seq, v)

	}()

	// the tests go quickly; allow concurrency calls to run
	time.Sleep(10 * time.Millisecond)
}

func (px *Paxos) PutDone(args *DoneArgs, reply *DoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.donePeers[args.Me] = args.MaxValue

	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
// see the comments for Min() for more explanation.
//
// @eburdon: following paragraph shamelessly stolen from
// http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html
//
// Your Paxos should implement freeing of instances in the following way:
// * When a particular peer application will no longer need to call Status()
// for any instance <= x, it should call Done(x).
// * That Paxos peer can't yet discard the instances, since some other
// Paxos peer might not yet have agreed to the instance.
// * So each Paxos peer should tell each other peer the highest Done argument
// supplied by its local application. Each Paxos peer will then have a Done
// value from each other peer. It should find the minimum, and discard all instances
// with sequence numbers <= that minimum. The Min() method returns this minimum
// sequence number plus one.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()

	// update my own done value
	if seq > px.donePeers[px.me] {
		px.donePeers[px.me] = seq
	}

	args := &DoneArgs{}
	args.Me = px.me
	args.MaxValue = px.donePeers[px.me]

	px.mu.Unlock()

	// tell my peers what my latest & greatest done value is
	for me, peer := range px.peers {

		var reply DoneReply

		if me == px.me {
			continue
		}

		call(peer, "Paxos.PutDone", args, &reply)
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {

	px.mu.Lock()
	defer px.mu.Unlock()

	max := 0

	for k, _ := range px.recProposals {
		if k > max {
			max = k
		}
	}

	return max
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

	min := px.donePeers[px.me]

	for _, v := range px.donePeers {
		if v < min {
			min = v
		}
	}

	for k, v := range px.recProposals {
		if k >= min {
			continue
		}

		if v.Fate != Decided {
			continue
		}

		delete(px.recProposals, k)
		fmt.Println("Deleted entry at", k)
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status(seq)
// should just inspect the local peer state;
// it should not contact other Paxos peers.
// caveat: If Status() is called with a sequence
// number less than Min(), Status()should return Forgotten.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}

	// reading lock
	px.mu.Lock()
	defer px.mu.Unlock()

	_, key_exists := px.recProposals[seq]

	if !key_exists {
		return Pending, nil
	}

	// fmt.Println("STATUS returning...", px.me, seq, px.recProposals[seq].PropNum, px.recProposals[seq].Fate, px.recProposals[seq].Value)

	return px.recProposals[seq].Fate, px.recProposals[seq].Value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

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

	// Your initialization code here.
	px.recProposals = make(map[int]*Proposal)

	px.donePeers = make(map[int]int)

	// initialize done values to -1
	for k, _ := range px.peers {
		px.donePeers[k] = -1
	}

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

		// please do not change any of the following code,
		// or do anything to subvert it.

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

	fmt.Printf("Finish\n")
	return px
}
