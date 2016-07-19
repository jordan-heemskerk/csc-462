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
	Cmd string
	Key string
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
	Log 		[]Op

	// map to maintain Key, Value pairs
	KeyVals		map[string]string

	// map of sequence values seen
	Seqs		map[int64]bool

	// latest PAXOS sequence
	PSeq 		int
}

// shamelessly stole this fucntion from 3A test_test.go
// wait for paxos to finish deciding!
func (kv *KVPaxos) wait(seq int) {
	to := 10 * time.Millisecond
	for {
		fate, v :=  kv.px.Status(seq)
		if fate == paxos.Decided {
			fmt.Println("WAIT RETURNED:", v)
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
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// RPC'd from client.go. Add mutex protection
	kv.mu.Lock()
	defer kv.mu.Unlock()

	operation := new(Op)
	operation.Cmd = "Get"
	operation.Key = args.Key

	// interpret to make sure my current log is up to date...

	// append GET to log
	kv.AddToLog(*operation)

	// TODO (later??) Dont manage each vote individually
	// Whatever Ryan was talking about... ?

	fmt.Println(kv.Log)

	// if _, ok := kv.Log[args.Key]; ok {
	// 	reply.Value = kv.Log[args.Key].Value
	// } else {
	// 	reply.Err = "Value does not exist! Returning empty value."
	// 	reply.Value = ""
	// 	fmt.Println(kv.Log)
	// 	fmt.Println(kv.me)
	// } 

	return nil
}


func (kv *KVPaxos) AddToLog(args Op) {
	kv.Log = append(kv.Log, args)

	fmt.Println("\n\tUpdated log with: ", kv.Log)
	fmt.Println(kv.me)
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
	if (exists) {
		reply.Err = "OK"
		return nil
	}

	// create operation
	operation := new(Op)
	operation.Key = args.Key
	operation.Value = args.Value
	operation.Cmd = args.Op

	// PROCESS OPERATION

	// get the latest sequence from the server; has this
	// sequence already been decided?
	fate, _ := kv.px.Status(kv.PSeq)

	if fate == paxos.Decided {
		fmt.Println("This sequence has already been voted on")
	} else {
		// Start paxos! (returns immediately)
		kv.px.Start(int(args.Seq), operation)
		kv.wait(int(args.Seq))

		// if it succeeds, add to log.
		kv.AddToLog(*operation)
	}

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
