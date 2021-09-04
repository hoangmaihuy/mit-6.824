package kvraft

import (
	"sync"
)

type ApplyResult struct {
	op    Op
	value string
	err   Err
}

type ApplyData struct {
	mu     sync.Mutex
	cond   *sync.Cond
	result map[int]ApplyResult
}

func (kv *KVServer) getApplyResultL(index int) *ApplyResult {
	elem, ok := kv.applyData.result[index]
	if !ok {
		return nil
	} else {
		return &elem
	}
}

func (kv *KVServer) apply(command interface{}) (string, Err) {
	op := command.(Op)
	switch op.OpName {
	case "Get":
		return kv.db.Get(op.Key)
	case "Put":
		return kv.db.Put(op.Key, op.Value)
	case "Append":
		return kv.db.Append(op.Key, op.Value)
	}
	return "", ""
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.applyData.mu.Lock()
			index := msg.CommandIndex
			value, err := kv.apply(msg.Command)
			kv.applyData.result[index] = ApplyResult{
				op:    msg.Command.(Op),
				value: value,
				err:   err,
			}
			kv.applyData.mu.Unlock()
			//DPrintf("[server %v] applier release lock", kv.me)
			kv.applyData.cond.Broadcast()
		}
	}
}
