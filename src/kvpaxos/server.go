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
	op_num int
	store  map[string]string
}

func (kv *KVPaxos) doPutAppend(cmd string, key string, val string) {

	// DO NOT LOCK THE MUTEX HERE

	if cmd == "Put" {
		kv.store[key] = val
	}

	if cmd == "Append" {
		kv.store[key] += val
	}

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {

	// RPC'd from client.go. Add mutex protection
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("\nKv Server Get!")

	// enter PUT op on paxos log
	// i.e., use Paxos to allocate a Paxos instance, whose value includes the
	// key and value

	// your server should try to assign the next available Paxos instance
	// (sequence number) to each incoming client RPC

	var start_args Op
	start_args.Key = args.Key
	start_args.Cmd = "Get"

	// kv.px is an actual PAXOS instance that has started; we don't need
	// to RPC to our other server.
	kv.px.Start(kv.op_num, start_args)

	fmt.Printf("Trying operation (Op: %s, K: %s, V: %s) for %d\n", start_args.Cmd, start_args.Key, start_args.Value, kv.op_num)
	for {
		fate, val := kv.px.Status(kv.op_num)

		fmt.Printf("fate: %d\n", fate)
		if fate != paxos.Pending {

			end_args := val.(Op)

			if end_args.Cmd != "Get" {

				fmt.Printf("Decided to do (Op: %s, K: %s, V: %s) for %d\n", end_args.Cmd, end_args.Key, end_args.Value, kv.op_num)
				kv.doPutAppend(end_args.Cmd, end_args.Key, end_args.Value)
			} else {
				fmt.Printf("Skip Get for %d\n", kv.op_num)
			}

			kv.op_num++
			if end_args == start_args {
				// may proceed with the Get now

				if val, ok := kv.store[end_args.Key]; ok {
					reply.Value = val
					fmt.Printf("Get returning %s\n", val)
				} else {
					reply.Err = ErrNoKey
				}

				break

			}

		}

		fmt.Printf("Get: Another one\n")
		time.Sleep(10 * time.Millisecond)

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
	// RPC'd from client.go. Add mutex protection
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("\nKv Server PutAppend!")

	// enter PUT op on paxos log
	// i.e., use Paxos to allocate a Paxos instance, whose value includes the
	// key and value

	// your server should try to assign the next available Paxos instance
	// (sequence number) to each incoming client RPC

	var start_args Op
	start_args.Key = args.Key
	start_args.Value = args.Value
	start_args.Cmd = args.Op

	// kv.px is an actual PAXOS instance that has started; we don't need
	// to RPC to our other server.
	kv.px.Start(kv.op_num, start_args)

	fmt.Printf("Trying operation (Op: %s, K: %s, V: %s) for %d\n", start_args.Cmd, start_args.Key, start_args.Value, kv.op_num)

	for {
		fate, val := kv.px.Status(kv.op_num)

		fmt.Printf("fate: %d\n", fate)
		if fate != paxos.Pending {

			end_args := val.(Op)

			fmt.Printf("Got (Op: %s, K: %s, V: %s) for %d\n", end_args.Cmd, end_args.Key, end_args.Value, kv.op_num)

			if end_args.Cmd != "Get" {

				fmt.Printf("Decided to do (Op: %s, K: %s, V: %s) for %d\n", end_args.Cmd, end_args.Key, end_args.Value, kv.op_num)
				kv.doPutAppend(end_args.Cmd, end_args.Key, end_args.Value)
			} else {
				fmt.Printf("Skip Get for %d\n", kv.op_num)
			}

			kv.op_num++
			if end_args == start_args {
				break
			} else {
				kv.px.Start(kv.op_num, start_args)
			}

		}
		fmt.Printf("Put: Another one\n")
		time.Sleep(10 * time.Millisecond)

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
	kv.op_num = 1
	kv.store = make(map[string]string)

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
