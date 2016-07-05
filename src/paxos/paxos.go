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
// import "errors"
// import "encoding/gob"

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
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here

	// track the proposals I have received
	recProposals map[int]Proposal
    minValue int // the incoming value must be at least this big // todo: delete?
}

type Proposal struct {
	Seq      int
	Value    interface{}
	Fate     Fate
	Majority int // number that must be matched or beaten to be accepted
}

//
// I want to return my status?
// If I am currently in consensus, or a intermediate state?
//
type InterrogationReply struct {
	Seq int 
    Value interface{}
    Error string
}

//
// TODO: Do I need to return anything from a decision call?
//
type AcceptanceReply struct {
	Test int
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
// Decision call! The passed in value has won the vote!
//
func (px *Paxos) Decide(args *Proposal, reply *AcceptanceReply) error {

	proposal := px.recProposals[args.Seq]
	proposal.Fate = args.Fate
	px.recProposals[args.Seq] = proposal

	// TODO: error handling?!
	return nil
}

//
// Interrogation call
//      If current sequence value is greater than what I've already seen,
//      I promise to accept.
//      If its the same or less, I must reject
// TODO?
//      If I am in an intermediate state? If I am in a consensus state, ready
//      for another value?
//      ** I don't want to clean values here! Only check!
//
func (px *Paxos) AcceptPromise(args *Proposal, reply *InterrogationReply) error {

    max := px.Max()

    // build reply [always]
    reply.Seq = max
    reply.Value = px.recProposals[max].Value
    // reply.Error = nil

    if args.Seq > max ||  max == 0 {
        // save proposal
        px.recProposals[args.Seq] = *args
    } else {
        reply.Error = "sequence value is too small / out of date. Rejected!"
    }

    return nil
}

func (px *Paxos) doesKeyExist (seq int) bool {
    exists := false

    for p, _ := range px.recProposals {
        if seq == p {
            exists = true
            break
        }
    }

    return exists
}

//
// Similar to decide; I add the current sequence to my list, and
// give a pending fate
//
func (px *Paxos) Update(seq int) {
    if px.doesKeyExist(seq) == false {
        proposal := px.recProposals[seq]
        proposal.Fate = Pending
        px.recProposals[seq] = proposal
    }
}

func calculateMajority(peerCount int) int {
	temp := float64(peerCount) / 2.0
	return int(math.Ceil(temp))
}

//
// the application wants paxos to start agreement/
// conensus on instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// create proposal
	prop := &Proposal{}
	prop.Seq = seq
	prop.Value = v
	prop.Fate = Pending
    prop.Majority = calculateMajority(len(px.peers))

    // fmt.Println("\tMajority needed is:", prop.Majority)

    // latest agreed upon sequence and value of acceptor
	var my_reply InterrogationReply

	for i := 0; i < len(px.peers); i++ {

		// fmt.Println("\tCalling ... ", px.peers[i])
        // fmt.Println("\tSequence...", seq)
        // fmt.Println("\tValue...", v)

		if ok := call(px.peers[i], "Paxos.AcceptPromise", prop, &my_reply); !ok {

			fmt.Println("Failed Accept!")

            // remove from peers [i.e., prevent DECIDE from being called]
            newPeers := append(px.peers[:i], px.peers[i+1:]...)
            px.peers = newPeers

            // recalculate majority
            prop.Majority = calculateMajority(len(px.peers))
            // fmt.Println("\tNew majority needed is:", prop.Majority)

		} else if my_reply.Error != "" {
            // reach this stage if, e.g., my sequence number is out of date (old)
            // the latest information is in my_reply. This proposal will not be saved.
            // MERMAID
            fmt.Println(seq, my_reply)

            // instead, update with latest values
            px.Update(seq)

        } else {
			// increment the number of servers that have agreed to accept
			// TODO: Make atomic
			prop.Majority--
		}

		// as soon as we have a majority, mark the proposal as
		// decided, and sent out.
		if prop.Majority == 0 {
			fmt.Println("\t\tWe have majority on: ", seq)
			prop.Fate = Decided
		}
	}

	// TODO: Move into function
	// if we have a majority, finalize agreement
	if prop.Fate == Decided {

		var my_reply AcceptanceReply

		for i := 0; i < len(px.peers); i++ {
			if ok := call(px.peers[i], "Paxos.Decide", prop, &my_reply); !ok {
				fmt.Println("\nFAIL DECIDE\n")
            } else {
				// TODO: increase done count?
                // TODO: Accept me? here or in DECIDE?
				// What do?
			}
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.

	// TODO: map should have a MAX functionality
	// TODO: THIS IS NOT ROBUST?!
	// otherwise., assume to return the last item in the map?
	// max := len(px.recProposals) - 1

    max := 0

    for k, _ := range px.recProposals {
        if k > max {
            max = k
        }
    }

	// return 0
    // fmt.Println("\tMax...", max)
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// TODO: when/why does it call Done() ?
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// TODO: Periodically check this and DELETE from map!
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
// TODO: keep track of a server's DONE sequence value!
// TODO: Keep track of which ones have 'heard' the min?
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,        [any instance??]
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

    // fmt.Println("Status\n\n")

    max := px.Max()

    if px.doesKeyExist(seq) == false {
        fmt.Println("THE PASSED IN VALUE DOES NOT EXIST:", seq)

        // What do I want to happen when I don't have a proposal record?
        // I want to return the value of the instance I *have* agreed on, if exists
        // [e.g., I want to reveal if I have reached a consensus at all]
        var v interface{} = px.recProposals[max].Value
        return px.recProposals[max].Fate, v
    }

    // max := px.Max()
    instance := seq

    if max > seq {
        // the passed in sequence is out of date
        fmt.Println("The passed in sequence of out of date. Actual: ", max)
        instance = max
    }

    // this current checks the most recently passed in value
    if px.recProposals[max].Fate == Decided {
        instance = max
    }

    var v interface{} = px.recProposals[instance].Value
    return px.recProposals[instance].Fate, v
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
	px.recProposals = make(map[int]Proposal)

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

	return px
}
