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

	// map to maintain Key, Value pairs; "f"
	KeyVals map[string]string

	// latest PAXOS sequence; send to server!
	PSeq int

	// todo: later -- don't store the value?! But we need!
	SelfLog []Op
}

//
// Shamelessly stolen from the lab writeup; wait for PAXOS to return
//
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
//	Apply an external operation to my database
//
func (kv *KVPaxos) Apply(operation Op, seq int) {

	if operation.Cmd == "Get" {
		kv.px.Done(seq)
		return
	}

	if operation.Cmd == "Append" {
		value, exists := kv.KeyVals[operation.Key]

		if !exists {
			kv.KeyVals[operation.Key] = operation.Value
		} else {
			value = value + operation.Value
			kv.KeyVals[operation.Key] = value
		}

	} else if operation.Cmd == "Put" {
		kv.KeyVals[operation.Key] = operation.Value

	} else {
		fmt.Println("\n\n\tUnknown Command. Something went terribly terribly wrong.\n\n")
	}

	// call the Paxos Done() method when a kvpaxos has processed an instance and
	// will no longer need it or any previous instance.
	kv.px.Done(seq)
}

//
// What value is my server at? What value does my PAXOS think it's at?
// If I'm already there; return
// Otherwise, retrieve the PAXOS log. If decided, apply.
//
func (kv *KVPaxos) Synchronize(currSeq int) int {
	max := kv.px.Max()

	if currSeq == max {
		return max + 1
	}

	for i := currSeq; i <= max; i++ {
		fate, item := kv.px.Status(i)

		if fate == paxos.Decided {
			operation, ok := item.(Op)

			if !ok {
				operation, _ := item.(*Op)
				kv.Apply(*operation, i)
			} else {
				kv.Apply(operation, i)
			}
		}
	}
	return max + 1
}

//
// Go through all items in my self log and apply to my key value database
// once finished, clear the log so I dont do it again
//
func (kv *KVPaxos) ApplyLog() {
	for i := 0; i < len(kv.SelfLog); i++ {
		operation := kv.SelfLog[i]

		if operation.Cmd == "Append" {
			value, exists := kv.KeyVals[operation.Key]

			if !exists {
				kv.KeyVals[operation.Key] = operation.Value
			} else {
				value = value + operation.Value
				kv.KeyVals[operation.Key] = value
			}

		} else if operation.Cmd == "Put" {
			kv.KeyVals[operation.Key] = operation.Value

		} else {
			fmt.Println("\n\n\tUnknown Command. Something went terribly terribly wrong.\n\n")
		}
	}

	kv.SelfLog = []Op{}
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
	fmt.Println("(2) KV Server Get!")

	operation := new(Op)
	operation.Cmd = "Get"
	operation.Key = args.Key

	kv.mu.Lock()

	startSeq := kv.Synchronize(kv.PSeq)

	kv.ApplyLog()

	kv.px.Start(startSeq, operation)

	_ = kv.wait(startSeq)

	kv.mu.Unlock()

	// get and return the value
	if value, ok := kv.KeyVals[operation.Key]; ok {
		reply.Value = value

	} else {
		reply.Err = "KvPaxos Get:\tValue does not exist! Returning empty."
		reply.Value = ""
	}

	kv.px.Done(startSeq)
	kv.PSeq = startSeq + 1

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
	fmt.Println("(2) KV Server PutAppend!")

	// create operation
	operation := new(Op)
	operation.Key = args.Key
	operation.Value = args.Value
	operation.Cmd = args.Op

	mySeq := kv.px.Max() + 1

	// call paxos; append on majority
	kv.mu.Lock()

	kv.px.Start(mySeq, operation)

	what := kv.wait(mySeq)

	fmt.Println(what)

	// todo: check that WAIT matches OPERATION?
	kv.SelfLog = append(kv.SelfLog, *operation)

	kv.PSeq++

	kv.mu.Unlock()

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

	kv.SelfLog = []Op{}

	// start with seq 0; monotonically increasing
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
