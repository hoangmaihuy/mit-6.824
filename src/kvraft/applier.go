package kvraft

import (
	"6.824/raft"
	"sync"
)

type ApplyData struct {
	mu sync.Mutex
	cond *sync.Cond
	data map[int]raft.ApplyMsg
}

func (kv *KVServer) getMessageL(index int) *raft.ApplyMsg {
	elem, ok := kv.applyData.data[index]
	if !ok {
		return nil
	} else {
		return &elem
	}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			kv.applyData.mu.Lock()
			//DPrintf("[server %v] applier received message = %v", kv.me, msg)
			_, ok := kv.applyData.data[index]
			if ok {
				DPrintf("[server %v] two apply message with same index = %v", kv.me, msg.CommandIndex)
			} else {
				kv.applyData.data[index] = msg
				kv.applyData.mu.Unlock()
				//DPrintf("[server %v] applier release lock", kv.me)
				kv.applyData.cond.Broadcast()
			}
		}
	}
}
