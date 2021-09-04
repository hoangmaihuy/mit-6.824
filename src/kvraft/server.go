package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Id     int
	OpName string // "Get", "Put", "Append"
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opCount   int
	applyData ApplyData
	db        Database
	log       map[string]Op
	resp      map[int64]interface{}
}

func (kv *KVServer) getOpId() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.opCount++
	return kv.opCount
}

func (kv *KVServer) waitCommit(opId int, index int, term int) bool {
	kv.applyData.mu.Lock()
	defer kv.applyData.mu.Unlock()
	for {
		//DPrintf("[server %v] waitCommit opId = %v, index = %v", kv.me, opId, index)
		msg := kv.getMessageL(index)
		if msg == nil {
			//DPrintf("[server %v] waitCommit sleep opId = %v, index = %v", kv.me, opId, index)
			kv.applyData.cond.Wait()
		} else {
			currentTerm, _ := kv.rf.GetState()
			if msg.Command.(Op).Id == opId && currentTerm == term {
				DPrintf("[server %v] should commit opId = %v, index = %v", kv.me, opId, index)
				delete(kv.applyData.data, index)
				return true
			} else {
				DPrintf("[server %v] should NOT commit opId = %v, index = %v", kv.me, opId, index)
				return false
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Index = args.Index
	if resp := kv.checkReply(args.ClientId, args.Index, "Get"); resp != nil {
		reply.Err = resp.(GetReply).Err
		reply.Value = resp.(GetReply).Value
		DPrintf("[server %v] Get: args = %v, reply = %v", kv.me, args, reply)
		return
	}
	op := Op{
		Id:     kv.getOpId(),
		OpName: "Get",
		Key:    args.Key,
		Value:  "",
	}
	var shouldCommit bool
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shouldCommit = kv.waitCommit(op.Id, index, term)
	if shouldCommit {
		value, err := kv.db.Get(args.Key)
		reply.Value = value
		reply.Err = err
		kv.saveReply(args.ClientId, *reply)
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[server %v] Get: args = %v, reply = %v", kv.me, args, reply)
}

func (kv *KVServer) checkReply(clientId int64, index int, op string) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply, ok := kv.resp[clientId]
	if ok {
		switch reply.(type) {
		case GetReply:
			if op == "Get" && reply.(GetReply).Index == index {
				return reply
			} else {
				return nil
			}
		case PutAppendReply:
			if op == "PutAppend" && reply.(PutAppendReply).Index == index {
				return reply
			} else {
				return nil
			}
		default:
			return nil
		}
	} else {
		return nil
	}
}

func (kv *KVServer) saveReply(clientId int64, reply interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.resp[clientId] = reply
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Index = args.Index
	if resp := kv.checkReply(args.ClientId, args.Index, "PutAppend"); resp != nil {
		reply.Err = resp.(PutAppendReply).Err
		DPrintf("[server %v] PutAppend: args = %v, reply = %v", kv.me, args, reply)
		return
	}
	op := Op{
		Id:     kv.getOpId(),
		OpName: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}
	var shouldCommit bool

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		goto ReplyWrongLeader
	}
	shouldCommit = kv.waitCommit(op.Id, index, term)
	if shouldCommit {
		if args.Op == "Put" {
			kv.db.Put(args.Key, args.Value)
		} else {
			kv.db.Append(args.Key, args.Value)
		}
		goto ReplyOK
	} else {
		goto ReplyWrongLeader
	}

ReplyWrongLeader:
	reply.Err = ErrWrongLeader
	DPrintf("[server %v] PutAppend: args = %v, reply = %v", kv.me, args, reply)
	return
ReplyOK:
	reply.Err = OK
	kv.saveReply(args.ClientId, *reply)
	DPrintf("[server %v] PutAppend: args = %v, reply = %v", kv.me, args, reply)
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("[server %v] killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db.data = make(map[string]string)

	kv.applyData.mu = sync.Mutex{}
	kv.applyData.cond = sync.NewCond(&kv.applyData.mu)
	kv.applyData.data = make(map[int]raft.ApplyMsg)

	kv.resp = make(map[int64]interface{})
	go kv.applier()
	return kv
}
