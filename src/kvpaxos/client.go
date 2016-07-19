package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"

import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	var reply GetReply

	args := new(GetArgs)
	args.Key = key

	var peer string

	if len(ck.servers) > 0 {
		peer = ck.servers[0]
	} else {
		fmt.Println("\nWE HAVE NO KVPAXOS SERVERS TO CONTACT. EXIT.\n")
		return ""
	}

	fmt.Println(peer)

	if ok := call(peer, "KVPaxos.Get", args, &reply); !ok {
		// failed RPC error handling -- try another server?
		fmt.Println("RPC Call literally failed.")

	} else if reply.Err != "" {
		// error handling -- try another server?
		fmt.Println("RPC Returned an Error")

	} else {
		// success
		fmt.Println("Call succeeded")
	}

	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// RPC Our kvpaxos server!
	var reply PutAppendReply

	args := new(PutAppendArgs)
	args.Key = key
	args.Value = value
	args.Op = op

	var peer string

	if len(ck.servers) > 0 {
		peer = ck.servers[0]
	} else {
		fmt.Println("\nWE HAVE NO KVPAXOS SERVERS TO CONTACT. EXIT.\n")
		return
	}

	fmt.Println(peer)

	if ok := call(peer, "KVPaxos.PutAppend", args, &reply); !ok {
		// failed RPC error handling -- try another server?
		fmt.Println("RPC Call literally failed.")

	} else if reply.Err != "" {
		// error handling -- try another server?
		fmt.Println("RPC Returned an Error")

	} else {
		// success
		fmt.Println("Call succeeded")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// RPC TEMPLATE
// if ok := call(peer, "KVPaxos.PutAppend", args, &reply); !ok {
// 	// failed RPC error handling
// 	fmt.Println("RPC Call literally failed.")
// } else if reply.Err != "" {
// 	// error handling
// 	fmt.Println("RPC Returned an Error")
// } else {
// 	// success
// 	fmt.Println("Call succeeded")
// }
