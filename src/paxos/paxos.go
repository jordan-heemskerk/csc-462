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
    
    // each paxos can be any of: proposer, acceptor, listener. Implement state data.
    Seq_p int
    Seq_a int 
    Value_a interface{}
}

type Proposal struct {
	Seq      int
	Value    interface{}
	Fate     Fate
	Majority int // number that must be matched or beaten to be accepted
}

// the propose stage is also an interrogation stage
// I reply with the highest sequence and value pair I have seen
type InterrogationReply struct {
    Seq int 
    Value interface{}
    Error string
}

// on accept_ok, I return the sequence number I have accepted
type AcceptanceReply struct {
    Seq int
	Error string
}

// no reponse needed from Decide
type DecideReply struct{ 
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
// No response! The passed in value is FINAL.
//
func (px *Paxos) Decide(args *Proposal, reply *DecideReply) error {
    // fmt.Println("DECIDE")

    // update item in my received proposals
    proposal := px.recProposals[args.Seq]
    proposal.Fate = Decided
    px.recProposals[args.Seq] = proposal

	return nil
}

//
// Return accept_ok or accept_reject
// OK if sequence is GREATER OR EQUAL to what I've already seen
// else, I must reject
//
func (px *Paxos) Accept(args *Proposal, reply *AcceptanceReply) error {
    // fmt.Println("ACCEPT")

    if args.Seq >= px.Seq_p {
        // accept_ok
        px.Seq_p = args.Seq
        px.Seq_a = args.Seq
        px.Value_a = args.Value

        reply.Seq = px.Seq_p
    } else {
        // accept_reject
        reply.Error = "sequence value is too small / out of date. Accept Rejected!"
    }

    return nil;
}

//
// Return propose_ok or propose_reject
// Interrogation call;  Ok if sequence value is GREATER than what I've already seen,
//                      Else, I must reject.
func (px *Paxos) Propose(args *Proposal, reply *InterrogationReply) error {
    // fmt.Println("PROPOSE")

    max := px.Max()

    if args.Seq > max ||  max == 0 {
        // propose_ok
        // respond with highest accepted sequence, highest accepted value
        reply.Seq = px.Seq_a
        reply.Value = px.Value_a

        // save proposal; I must keep track of what proposals I have received
        px.recProposals[args.Seq] = *args
    } else {
        // propose_reject
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
	// prop.Fate = Pending
    prop.Majority = calculateMajority(len(px.peers)) // for propose_ok

    fmt.Println("We need a majority on: ", prop.Majority)

    /* SEND PROPOSAL */
    // send proposal to all peers, including myself [me first] - TODO

    var peer_reply InterrogationReply

    pok_count := 0

	for i := 0; i < len(px.peers); i++ {

		// fmt.Println("\tCalling ... ", px.peers[i])
        // fmt.Println("\tSequence...", seq)
        // fmt.Println("\tValue...", v)

        // send propose; receive propose_ok
		if ok := call(px.peers[i], "Paxos.Propose", prop, &peer_reply); !ok {
            // CALL FAILED
            // we enter this if, for example, the peer dies and the actual RPC call fails

            // remove from peers
            newPeers := append(px.peers[:i], px.peers[i+1:]...)
            px.peers = newPeers

            // assuming peer is dead, recalculate majority
            prop.Majority = calculateMajority(len(px.peers))

		} else if peer_reply.Error != "" {
            // propose_reject
            // we enter if, e.g., my sequence number is out of date (old)
            // intead of pushing for a majority, update the sequence / proposal
            px.Update(seq)

        } else {
            // propose_ok
            // maintain a count of the peers that have agreed to accept something
            // I can either send MY value, or the one currently holding consensus
            // (e.g., propgate it)
			pok_count++;
		}

		// as soon as we have a majority, mark the proposal as decided
		if pok_count == prop.Majority  {
			prop.Fate = Pending
		}
	}

    /* SEND ACCEPT */
    // with majority on current proposal, send accept; receive accept_ok

    // to be used in majority
    var accept_reply AcceptanceReply

    aok_count := 0

	if prop.Fate == Pending {
		for i := 0; i < len(px.peers); i++ {

			if ok := call(px.peers[i], "Paxos.Accept", prop, &accept_reply); !ok {
                // call failed!
				fmt.Println("ACCEPT RPC FAILED")

            } else if accept_reply.Error != "" {
                // accept_reject
                fmt.Println("WE HAVE AN ACCEPT REJECT")

			} else {
                // accept_ok success
                aok_count++;
            }
		}
	}

    if aok_count >= prop.Majority {
        prop.Fate = Decided
    }

    /* SEND DECIDE */
    // if we have received accept_ok from majority, send decide

    var decide_reply DecideReply

    if prop.Fate == Decided {
        for i := 0; i < len(px.peers); i++ {

            if ok := call(px.peers[i], "Paxos.Decide", prop, &decide_reply); !ok {
                // call failed!
                fmt.Println("DECIDE RPC FAILED")
            } else {
                // success; we should be all done
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
    // TODO: This is wrong? Should it just be px.Seq_p ?
    max := 0

    for k, _ := range px.recProposals {
        if k > max {
            max = k
        }
    }

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

    fmt.Println("Status")
    fmt.Println(px.me)

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
