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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Cmd   string
	Key   string
	Value string
	Hash  int64
}

type OpReply struct {
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// map to maintain Key, Value pairs; "f"
	KeyVals map[string]string

	// Keep track of which operations have been applied
	AppliedHash map[int64]bool

	// latest PAXOS sequence; send to server!
	PSeq int
}

//
// Shamelessly stolen from the lab writeup; wait for PAXOS to return
//
func (kv *KVPaxos) wait(seq int) interface{} {
	to := 10 * time.Millisecond
	for {

		fate, v := kv.px.Status(seq)

		fmt.Println("Wait for", seq, "on", kv.me, "=", fate)
		if fate == paxos.Decided {
			// convert / parse interface without panicking
			reply_value, _ := v.(*Op)
			fmt.Println("Got", seq)
			return reply_value
		}

		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
}

func val2op(val interface{}) Op {
	_, ok := val.(Op)

	if !ok {
		op, _ := val.(*Op)
		return *op
	} else {
		op, _ := val.(Op)
		return op
	}

}

func (kv *KVPaxos) doPaxos(op interface{}) {
	for {

		kv.mu.Lock()

		seq := kv.px.Max() + 1 // What the instance we will create will be
		kv.Sync(seq - 1)       // Need to sync up to the one before what we will be
		kv.px.Start(seq, op)

		kv.mu.Unlock()

		// No waiting on the paxos! (no sequential!)
		pxop := kv.wait(seq)

		if pxop == op {
			kv.mu.Lock()
			kv.Sync(seq) // will actually apply our now decided operation
			kv.mu.Unlock()
			break
		}

	}

}

func (kv *KVPaxos) Sync(max_seq int) {

	// protected upstream by a mutex

	fmt.Printf("%d: currently @ %d -> %d\n", kv.me, kv.PSeq, max_seq)

	for kv.PSeq <= max_seq {

		sleep_for := 10 * time.Millisecond

		for {

			fate, v := kv.px.Status(kv.PSeq)

			if fate == paxos.Decided {
				// Apply this operation

				operation := val2op(v)

				//fmt.Printf("Applying Operation %s on %d for %d\n", operation, kv.me, kv.PSeq)
				if _, applied := kv.AppliedHash[operation.Hash]; applied {
					// We have already applied this
					break
				}

				if operation.Cmd == "Get" {
					kv.AppliedHash[operation.Hash] = true
					break // we are good if its a get
				} else {

					if operation.Cmd == "Put" {

						// fmt.Println("Putting ", operation.Value, " on ", operation.Key)
						// do put
						kv.KeyVals[operation.Key] = operation.Value

					} else if operation.Cmd == "Append" {

						// fmt.Println("Putting ", operation.Value, " on ", operation.Key, "(", kv.KeyVals[operation.Key], ")")

						// do append
						if _, exists := kv.KeyVals[operation.Key]; exists {

							kv.KeyVals[operation.Key] += operation.Value

						} else {

							kv.KeyVals[operation.Key] = operation.Value

						}

					} else {

						log.Fatal("Unexpected operation!\n")
					}
					kv.AppliedHash[operation.Hash] = true
					break

				}

			}

			// Give PAXOS some time
			time.Sleep(sleep_for)

			// Gradually increase sleep more and more
			if sleep_for < 5*time.Second {
				sleep_for *= 2
			}

		}

		kv.PSeq++

	}

	kv.px.Done(kv.PSeq - 1)

}

//
// Implement a Get() handler. It should enter a Get Op in the Paxos log,
// and then "interpret" the the log before that point to make sure its
// key/value database reflects all recent Put()s.
//
// PROCESSING == Actually applying SelfLog to the database after "interpreting"
//				existing
//
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	fmt.Printf("KV Server Get key %s on %d!\n", args.Key, kv.me)

	operation := new(Op)
	operation.Cmd = "Get"
	operation.Key = args.Key
	operation.Hash = args.Hash

	// update
	kv.doPaxos(operation)

	// get and return the value
	if value, ok := kv.KeyVals[operation.Key]; ok {
		reply.Value = value

	} else {
		reply.Err = "KvPaxos Get:\tValue does not exist! Returning empty."
		reply.Value = ""
	}

	return nil
}

//
// I need to call Paxos.Start(key int, value interface)
// build key & interface
// RPC paxos server
// wait for decision (periodically call Status())
// return
//
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//fmt.Println("KV Server PutAppend! key %s value %s on %d", args.Key, args.Value, kv.me)

	// create operation
	operation := new(Op)
	operation.Key = args.Key
	operation.Value = args.Value
	operation.Cmd = args.Op
	operation.Hash = args.Hash

	kv.doPaxos(operation)

	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
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

// please do not change these two functions.
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
	// kv.Log = []Op{}
	kv.KeyVals = make(map[string]string)

	kv.AppliedHash = make(map[int64]bool)

	// start with seq 1; monotonically increasing
	kv.PSeq = 1

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

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
