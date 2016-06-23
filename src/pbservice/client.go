package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here

	View viewservice.View
}

type PutReq struct {
	Key   string
	Value string
	Op    string
	Time  string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	go func() {
		for ck.tick() {
			time.Sleep(viewservice.PingInterval)
		}
	}()

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

func (ck *Clerk) tick() bool {

	if view, ok := ck.vs.Get(); !ok {
		fmt.Printf("Get() failed in tick\n")
		return false
	} else {
		ck.View = view
		return true
	}

}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// fmt.Println("Clerk GET\n")

	// Your code here.

	get_args := &GetArgs{}
	get_args.Key = key

	var get_reply GetReply

	for {
		if ok := call(ck.View.Primary, "PBServer.Get", get_args, &get_reply); !ok {
			//	fmt.Printf("RPC Get() failed\n")
		} else {
			if get_reply.Err == ErrNoKey {
				return "???"
			}
			return get_reply.Value
		}
		time.Sleep(viewservice.PingInterval)
	}

	return "???"
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// THESE ARE INTERLEAVED!

	// RPC Get() on VS to find current view
	// Now I know the primary
	// RPC PutAppend() on the primary

	put_args := &PutAppendArgs{}
	put_args.Key = key
	put_args.Value = value
	put_args.Op = op
	put_args.Hash = nrand()

	for {
		// MERMAID
		// fmt.Printf("Client PutAppend\n")

		var put_reply PutAppendReply

		if ok := call(ck.View.Primary, "PBServer.PutAppend", put_args, &put_reply); ok {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
