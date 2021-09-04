package kvraft

import (
	"sync"
)

type ApplyKey struct {
	clientId int64
	index    int
}

type ApplyResult struct {
	op    Op
	value string
	err   Err
}

type ApplyData struct {
	mu     sync.Mutex
	cond   *sync.Cond
	result map[ApplyKey]ApplyResult
	done   map[int]bool
}

func (kv *KVServer) getApplyResultL(clientId int64, index int) *ApplyResult {
	key := ApplyKey{
		clientId: clientId,
		index:    index,
	}
	elem, ok := kv.applyData.result[key]
	if !ok {
		return nil
	} else {
		return &elem
	}
}

func (kv *KVServer) deleteApplyResultL(clientId int64, index int) {
	delete(kv.applyData.result, ApplyKey{
		clientId: clientId,
		index:    index,
	})
}
func (kv *KVServer) checkDoneL(index int) bool {
	done :=  kv.applyData.done[index]
	return done
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
			DPrintf("[server %v] apply msg = %v", kv.me, msg)
			key := ApplyKey{
				clientId: msg.Command.(Op).ClientId,
				index:    msg.Command.(Op).Index,
			}
			_, ok := kv.applyData.result[key]
			if !ok {
				value, err := kv.apply(msg.Command)
				kv.applyData.result[key] = ApplyResult{
					op:    msg.Command.(Op),
					value: value,
					err:   err,
				}
			}
			kv.applyData.done[msg.CommandIndex] = true
			kv.applyData.mu.Unlock()
			//DPrintf("[server %v] applier release lock", kv.me)
			kv.applyData.cond.Broadcast()
		}
	}
}
