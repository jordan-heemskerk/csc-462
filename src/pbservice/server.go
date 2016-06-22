package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your deClarations here.

	view viewservice.View
	db   map[string]string
	ops  map[int64]bool

	putappendmu sync.Mutex
}

func (pb *PBServer) TransferDB(args *TransferDBArgs, reply *TransferDBReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	reply.Db = make(map[string]string)

	// We need to copy
	for k, v := range pb.db {
		reply.Db[k] = v
	}

	return nil

}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	fmt.Println("Server GET\n")

	key := args.Key

	if val, ok := pb.db[key]; ok {
		reply.Err = OK
		reply.Value = val
		return nil
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
		return errors.New("Key does not exist")
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	// Only ever atomically add things
	// is this too high?

	// Can't lock main mutex becuase we may have the backup change mid call
	pb.putappendmu.Lock()
	defer pb.putappendmu.Unlock()

	if _, ok := pb.ops[args.Hash]; ok {
		// ignore the request, put tell the client everything is coo'
		reply.Err = OK
		return nil
	}

	// TODO need more here
	if pb.view.Primary != pb.me && pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	for {
		// If there is a backup, we must forward any Put request to it
		if pb.view.Backup != "" && pb.view.Primary == pb.me {

			var for_reply PutAppendReply
			if ok := call(pb.view.Backup, "PBServer.PutAppend", args, &for_reply); !ok {
				// We retry until success now
				//fmt.Printf("Forwarding Put request failed.\n")
			} else {
				break
			}
			time.Sleep(100 * time.Millisecond)

		} else {
			break
		}
	}

	key := args.Key
	val := args.Value
	op := args.Op

	//fmt.Printf("Primary: %s, Backup: %s, Key: %s, Value: %s, Op: %s\n", pb.view.Primary, pb.view.Backup, key, val, op)
	if op == "Put" {
		//Put
		pb.db[key] = val
		reply.Err = OK
	}

	if op == "Append" {
		// Append
		if _, ok := pb.db[key]; ok {
			pb.db[key] += val
		} else {
			pb.db[key] = val
		}
		reply.Err = OK
	}

	pb.ops[args.Hash] = true

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	v, err := pb.vs.Ping(pb.view.Viewnum)

	if err != nil {
		fmt.Printf("Ping failed.\n")
	}

	if pb.view != v {

		pb.view = v

		// If I am now the backup, ask the primary to
		// transfer everything to me and then save it
		if v.Backup == pb.me {
			fmt.Printf("%s requests state transfer from %s \n", pb.me, v.Primary)

			tran_args := &TransferDBArgs{}
			var tran_reply TransferDBReply

			if ok := call(v.Primary, "PBServer.TransferDB", tran_args, &tran_reply); !ok {
				fmt.Printf("Transfer DB failed\n")
			} else {
				fmt.Printf("State transfer complete\n")
				// TODO!!!! Do we need to copy here? or is this ok
				pb.db = tran_reply.Db
			}

		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view.Primary = ""
	pb.view.Backup = ""
	pb.view.Viewnum = 0
	pb.db = make(map[string]string)
	pb.ops = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
