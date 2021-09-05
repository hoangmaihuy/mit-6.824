package raft

import (
	"6.824/labgob"
	"bytes"
)

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.setElectionTimeout()
	rf.DPrintf("InstallSnapshot: args = %v, reply = %v", args, reply)
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshotL(server int) {
	snapshot := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.DPrintf("sendInstallSnapshotL, server = %v, lastIncludedTerm = %v, lastIncludedIndex = %v", server, rf.lastIncludedTerm, rf.lastIncludedIndex)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		rf.setElectionTimeout()
		rf.updateTermL(reply.Term)
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.DPrintf("CondInstallSnapshot: term = %v, index = %v", lastIncludedTerm, lastIncludedIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastEntry := rf.lastLogEntry()
	lastTerm := lastEntry.Term
	lastIndex := lastEntry.Index
	if lastTerm < lastIncludedTerm || (lastTerm == lastIncludedTerm && lastIndex < lastIncludedIndex) {
		rf.currentTerm = lastIncludedTerm
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.updateTermL(lastIncludedTerm)
		go rf.Snapshot(lastIncludedIndex, snapshot)
		return true
	} else {
		rf.DPrintf("refuse install snapshot, lastTerm = %v, lastIndex = %v, snapshotTerm = %v, snapshotIndex = %v", lastTerm, lastIndex, lastIncludedTerm, lastIncludedIndex)
		//rf.applyCh <- ApplyMsg{
		//	CommandValid:  false,
		//	Command:       nil,
		//	CommandIndex:  0,
		//	SnapshotValid: false,
		//	Snapshot:      snapshot,
		//	SnapshotTerm:  lastIncludedTerm,
		//	SnapshotIndex: lastIncludedIndex,
		//}
		return false
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.DPrintf("begin snapshot: index = %v", index)
	rf.mu.Lock()
	lastIndex := rf.lastLogEntry().Index
	trimmedEntries := rf.getLogEntries(index, lastIndex-index+1)
	rf.lastIncludedIndex = index
	entry := rf.getLogEntry(index)
	if entry != nil {
		rf.lastIncludedTerm = entry.Term
	}
	rf.updateTermL(rf.lastIncludedTerm)
	if len(trimmedEntries) == 0 {
		trimmedEntries = append(trimmedEntries, Entry{
			Index: rf.lastIncludedIndex,
			Term: rf.lastIncludedTerm,
			Command: nil,
		})
	}
	rf.commitIndex = max(rf.commitIndex, index)
	rf.log.StartIndex = index
	rf.log.Entries = trimmedEntries
	// save raft state and snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.DPrintf("end snapshot: index = %v", index)
	rf.mu.Unlock()
}
