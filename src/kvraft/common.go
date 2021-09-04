package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId int64
	Index    int
	Key      string
	Value    string
	Op       string // "Put" or "Append"
}

type PutAppendReply struct {
	Index int
	Err   Err
}

type GetArgs struct {
	ClientId int64
	Index    int
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Index int
	Err   Err
	Value string
}
