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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd   string
	Key   string
	Value string
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

	// Your definitions here.

	// log should be a LIST, not a KV map
	// may potentially have the same operations on the same key
	Log []Op

	// map to maintain Key, Value pairs; "f"
	KeyVals map[string]string

	// map of sequence values seen
	Seqs map[int64]bool

	// latest PAXOS sequence
	PSeq int
}

// shamelessly stolen from the lab writeup
func (kv *KVPaxos) wait(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		fate, v := kv.px.Status(seq)

		if fate == paxos.Decided {
			// convert / parse interface without panicking
			reply_value, _ := v.(*Op)
			return reply_value
		}

		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
}

//
// Implement a Get() handler. It should enter a Get Op in the Paxos log,
// and then "interpret" the the log before that point to make sure its
// key/value database reflects all recent Put()s.
//
// TODO (later??) Dont manage each vote individually
// Whatever Ryan was talking about... ?
//
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// RPC'd from client.go. Add mutex protection
	kv.mu.Lock()
	defer kv.mu.Unlock()

	operation := new(Op)
	operation.Cmd = "Get"
	operation.Key = args.Key

	// add Get to log
	kv.Process(operation, args.Seq, kv.PSeq)

	// intepret values and log to see if correct

	// retrieve value
	if value, ok := kv.KeyVals[operation.Key]; ok {
		// reply.Err = "OK"
		reply.Value = value
	} else {
		reply.Err = "KvPaxos Get:\tValue does not exist! Returning empty."
		reply.Value = ""
	}

	return nil
}

func (kv *KVPaxos) Process(operation *Op, Seq int64, PSeq int) {
	// try infinitely
	for {
		var log_item interface{}

		// get the latest sequence from the server; has this
		// sequence already been decided?
		fate, value := kv.px.Status(PSeq)

		if fate == paxos.Decided {
			fmt.Println("This sequence has already been voted on. Ignoring.")

			log_item = value

		} else {
			// Start paxos! (returns immediately)
			kv.px.Start(int(Seq), operation)
			log_item = kv.wait(int(Seq))
		}

		if log_item == operation {
			// -- APPLY OPERATION -- //

			// add to log
			kv.Log = append(kv.Log, *operation)
			fmt.Println("\n\tUpdated log with: ", kv.Log)

			if operation.Cmd != "Get" {
				// update key / value store
				if operation.Cmd == "Append" {
					value, exists := kv.KeyVals[operation.Key]

					if !exists {
						kv.KeyVals[operation.Key] = operation.Value
					} else {
						value = value + operation.Value
						kv.KeyVals[operation.Key] = value
					}

				} else if operation.Cmd == "Put" {
					fmt.Println("\n\n\n\nPUTTING ", operation.Value, operation.Key)
					kv.KeyVals[operation.Key] = operation.Value

				} else {
					fmt.Println("\n\n\tUnknown Command. Something went terribly terribly wrong.\n\n")
				}
			}

			kv.Seqs[Seq] = true

			// call the Paxos Done() method when a kvpaxos has processed an instance and
			// will no longer need it or any previous instance.
			kv.px.Done(kv.PSeq)
			kv.PSeq++

			// exit for loop
			break
		} else {
			continue
		}
	}
}

//
// I need to call Paxos.Start(key int, value interface)
// build key & interface
// RPC paxos server
// wait for decision (periodically call Status())
// return
//
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// RPC'd from client.go. Add mutex protection
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("(2) KV Server PutAppend!")

	// my sequence should be unique!
	_, exists := kv.Seqs[args.Seq]
	if exists {
		reply.Err = "OK"
		return nil
	}

	// create operation
	operation := new(Op)
	operation.Key = args.Key
	operation.Value = args.Value
	operation.Cmd = args.Op

	// -- PROCESS OPERATION -- //
	kv.Process(operation, args.Seq, kv.PSeq)

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
	kv.Log = []Op{}
	kv.KeyVals = make(map[string]string)
	kv.Seqs = make(map[int64]bool)
	kv.PSeq = 0

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
