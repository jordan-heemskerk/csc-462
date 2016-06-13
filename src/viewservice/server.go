package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ServerStatus struct {
	Used     bool
	Lastping time.Time
}

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	View    View
	Servers map[string]*ServerStatus
	Acked   bool
}

func (vs *ViewServer) DeadServer(server string) {

	delete(vs.Servers, server)
	if vs.View.Primary == server {
		if vs.View.Backup == "" {
			log.Fatal("Primary failed and there's not a backup")
		} else {
			vs.NextView(vs.View.Backup, vs.NewServer())
		}
	}

	if vs.View.Backup == server {
		vs.NextView(vs.View.Primary, vs.NewServer())
	}
}

//
// go to a new view
//
func (vs *ViewServer) NextView(primary string, backup string) {

	if vs.View.Primary != primary || vs.View.Backup != backup {

		vs.View.Primary = primary
		vs.View.Backup = backup
		vs.View.Viewnum++
		vs.Acked = false

		fmt.Printf("Setting view %d (%s, %s)\n", vs.View.Viewnum, vs.View.Primary, vs.View.Backup)

	}

}

//
// get a new server
//
func (vs *ViewServer) NewServer() string {

	// Your code here.
	for me, server := range vs.Servers {

		if server.Used == false {
			vs.Servers[me].Used = true
			return me
		}

	}

	fmt.Println("Warning: No servers available")
	return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	fmt.Printf("Received Ping %d from %s\n", args.Viewnum, args.Me)

	// Find new servers
	if vs.Servers[args.Me] == nil {

		vs.Servers[args.Me] = new(ServerStatus)
		vs.Servers[args.Me].Used = false

	} else if args.Viewnum == 0 && vs.Servers[args.Me].Used {

		vs.DeadServer(args.Me)
		return nil

	}

	vs.Servers[args.Me].Lastping = time.Now()

	if vs.View.Primary == "" && vs.View.Backup == "" {

		vs.NextView(vs.NewServer(), "")

	}

	if vs.View.Primary != "" && vs.View.Backup == "" {

		vs.NextView(vs.View.Primary, vs.NewServer())

	}

	// Setup the reply
	reply.View = vs.View

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.View
	return nil

}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	for me, server := range vs.Servers {

		if time.Since(server.Lastping) > PingInterval*DeadPings && server.Used {

			fmt.Printf("%s died\n", me)
			vs.DeadServer(me)

		}

	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.Servers = make(map[string]*ServerStatus)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
