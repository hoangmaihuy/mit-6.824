package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientId int64
	Index    int
	OpName   string // "Get", "Put", "Append"
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyData ApplyData
	db        Database
}

func (kv *KVServer) waitApply(op *Op, index int) *ApplyResult {
	kv.applyData.mu.Lock()
	defer kv.applyData.mu.Unlock()
	for {
		if done := kv.checkDoneL(index); done {
			result := kv.getApplyResultL(op.ClientId, op.Index)
			if result == nil {
				return nil
			} else {
				//kv.deleteApplyResultL(op.ClientId, op.Index)
				return result
			}
		} else {
			kv.applyData.cond.Wait()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Index = args.Index
	if result := kv.checkApply(args.ClientId, args.Index); result != nil {
		reply.Err = result.err
		reply.Value = result.value
		DPrintf("[server %v] Get: args = %v, reply = %v", kv.me, args, reply)
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Index:    args.Index,
		OpName:   "Get",
		Key:      args.Key,
		Value:    "",
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitApply(&op, index)
	if result == nil {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Value = result.value
	reply.Err = result.err
	DPrintf("[server %v] Get: args = %v, reply = %v", kv.me, args, reply)
}

func (kv *KVServer) checkApply(clientId int64, index int) *ApplyResult {
	kv.applyData.mu.Lock()
	defer kv.applyData.mu.Unlock()
	return kv.getApplyResultL(clientId, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Index = args.Index
	if result := kv.checkApply(args.ClientId, args.Index); result != nil {
		reply.Err = result.err
		DPrintf("[server %v] PutAppend: args = %v, reply = %v", kv.me, args, reply)
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Index:    args.Index,
		OpName:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	var result *ApplyResult
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		goto ReplyWrongLeader
	}
	result = kv.waitApply(&op, index)
	if result == nil {
		goto ReplyWrongLeader
	}
	goto ReplyOK

ReplyWrongLeader:
	reply.Err = ErrWrongLeader
	DPrintf("[server %v] PutAppend: args = %v, reply = %v", kv.me, args, reply)
	return
ReplyOK:
	reply.Err = OK
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
	kv.applyData.result = make(map[ApplyKey]ApplyResult)
	kv.applyData.done = make(map[int]bool)

	go kv.applier()
	return kv
}
