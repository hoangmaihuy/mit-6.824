package raft

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := rf.lastEntry().Index
	rf.matchIndex[rf.me] = lastIndex
	for i := rf.commitIndex + 1; i <= lastIndex; i++ {
		matchCount := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				matchCount++
			}
		}
		//DPrintf("leader raft %v updateCommit: index = %v, matchCount = %v", rf.me, i, matchCount)
		if matchCount > len(rf.peers)/2 && rf.getEntry(i).Term == rf.currentTerm {
			rf.commitIndex = i
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex == rf.lastApplied {
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		rf.DPrintf("applier wakes up, commitIndex = %v, lastApplied = %v", commitIndex, rf.lastApplied)
		rf.mu.Unlock()
		for ; rf.lastApplied < commitIndex; rf.lastApplied++ {
			entry := rf.getEntry(rf.lastApplied+1)
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.Index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.DPrintf("applied entry = %v", entry)
		}
	}
}

