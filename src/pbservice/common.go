package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrKeyExists   = "ErrKeyExists"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op   string
	Hash int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type TransferDBArgs struct {
}

type TransferDBReply struct {
	Db map[string]string
}
