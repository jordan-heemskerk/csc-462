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
	View      View
	DeltaView View
	Servers   map[string]*ServerStatus
	Acked     bool

	RPCs uint

	start bool
}

func (vs *ViewServer) GetRPCCount() uint {
	return vs.RPCs
}

func (vs *ViewServer) DeadServer(server string) {

	if vs.View.Primary == server {
		if vs.View.Backup == "" {
			vs.View.Primary = ""
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

	if vs.Servers[primary] != nil {
		vs.Servers[primary].Used = true
	}

	if vs.Servers[backup] != nil {
		vs.Servers[backup].Used = true
	}

	if vs.View.Primary != primary || vs.View.Backup != backup {

		vs.DeltaView.Primary = primary
		vs.DeltaView.Backup = backup
		vs.DeltaView.Viewnum = vs.View.Viewnum + 1
		vs.Acked = false

		// set once on program load
		if vs.start == false {
			vs.start = true
		}

		fmt.Printf("Setting next view %d (%s, %s)\n", vs.DeltaView.Viewnum, vs.DeltaView.Primary, vs.DeltaView.Backup)

		if vs.DeltaView.Primary == vs.DeltaView.Backup && vs.DeltaView.Primary != "" {
			log.Fatal("Server cannot be primary and backup")
		}

	}

}

//
// get a new server
//
func (vs *ViewServer) NewServer() string {

	// Your code here.
	for me, server := range vs.Servers {

		if server.Used == false {
			fmt.Printf("Set %s as used\n", me)
			vs.Servers[me].Used = true
			if vs.View.Primary != me && vs.View.Backup != me {
				return me
			}
		}

	}

	//fmt.Println("Warning: No servers available")
	return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// fmt.Printf("Received Ping %d from %s\n", args.Viewnum, args.Me)

	if args.Viewnum == 0 && vs.Servers[args.Me] == nil && vs.View.Primary != "" {
		reply.View = vs.View
		return nil
	}

	// Find new servers
	if vs.Servers[args.Me] == nil {

		vs.Servers[args.Me] = new(ServerStatus)
		vs.Servers[args.Me].Used = false

	} else if args.Viewnum == 0 && vs.Servers[args.Me].Used {
		vs.Servers[args.Me].Used = false
		vs.DeadServer(args.Me)
		vs.Servers[args.Me].Lastping = time.Now()
		return nil

	}

	vs.Servers[args.Me].Lastping = time.Now()

	if !vs.Acked && args.Me == vs.DeltaView.Primary && args.Viewnum == vs.DeltaView.Viewnum {

		fmt.Println("View was acked")
		vs.Acked = true
		vs.View = vs.DeltaView

	}

	if vs.Acked && vs.View.Primary == "" && vs.View.Backup == "" {

		vs.NextView(vs.NewServer(), "")

	}

	if vs.Acked && vs.View.Primary != "" && vs.View.Backup == "" {

		vs.NextView(vs.View.Primary, vs.NewServer())

	}

	if args.Me != vs.DeltaView.Primary {
		reply.View = vs.View
	} else {
		reply.View = vs.DeltaView
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// We don't want to lock here because if we are waiting on having no primary, we may never get one
	//	vs.mu.Lock()
	//	defer vs.mu.Unlock()

	vs.RPCs++
	count := 0 // timeout retry count

	//fmt.Println("View service GET. Entering for loop\n")
	//fmt.Println("--", vs.View.Primary, vs.View.Backup)

	for vs.View.Primary == "" && vs.start == true {
		fmt.Printf("%d: %s, %s", count, vs.View.Primary, vs.View.Backup)
		time.Sleep(1000 * time.Millisecond)
		count++
		if vs.dead {
			return nil
		}
	}

	// fmt.Printf("Returning current view %d (%s, %s)", vs.View.Viewnum, vs.View.Primary, vs.View.Backup)
	reply.View = vs.View
	return nil

}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.
	for me, server := range vs.Servers {

		if time.Since(server.Lastping) > PingInterval*DeadPings && server.Used {

			fmt.Printf("%s died\n", me)
			server.Used = false
			delete(vs.Servers, me)
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
	vs.Servers = make(map[string]*ServerStatus)

	vs.start = false
	vs.Acked = true

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
