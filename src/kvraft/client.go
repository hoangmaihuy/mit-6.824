package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	mu       sync.Mutex
	leader   int
	clientId int64
	opIndex  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.opIndex = 0
	return ck
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) changeLeader() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) getOpIndex() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.opIndex++
	return ck.opIndex
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClientId: ck.clientId,
		Index:    ck.getOpIndex(),
		Key:      key,
	}
	for {
		leader := ck.getLeader()
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			DPrintf("[clerk to server %v] Get success: args = %v, reply = %v", leader, args, reply)
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.changeLeader()
			}
		} else {
			DPrintf("[clerk to server %v] Get failed: args = %v, reply = %v", leader, args, reply)
			ck.changeLeader()
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		Index:    ck.getOpIndex(),
		Key:      key,
		Value:    value,
		Op:       op,
	}
	for {
		leader := ck.getLeader()
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			DPrintf("[clerk to server = %v] PutAppend success: args = %v, reply = %v", leader, args, reply)
			switch reply.Err {
			case OK:
				return
			case ErrNoKey:
				return
			case ErrWrongLeader:
				ck.changeLeader()
			}
		} else {
			DPrintf("[clerk to server %v] PutAppend failed: args = %v, reply = %v", leader, args, reply)
			ck.changeLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
