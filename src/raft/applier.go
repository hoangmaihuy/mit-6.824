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
	lastIndex := rf.lastLogEntry().Index
	rf.matchIndex[rf.me] = lastIndex
	for i := lastIndex; i > rf.commitIndex; i-- {
		if rf.getLogEntry(i).Term == rf.currentTerm {
			matchCount := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i {
					matchCount++
					if matchCount > len(rf.peers)/2 {
						break
					}
				}
			}
			rf.DPrintf("updateCommit: index = %v, matchCount = %v", i, matchCount)
			if matchCount > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCond.Broadcast()
				break
			}
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex == rf.lastApplied {
			rf.DPrintf("applier sleep, commitIndex = %v, lastApplied = %v", rf.commitIndex, rf.lastApplied)
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.DPrintf("applier wakes up, commitIndex = %v, lastApplied = %v", commitIndex, rf.lastApplied)
		rf.mu.Unlock()
		for ; lastApplied < commitIndex; {
			rf.mu.Lock()
			entry := rf.getLogEntry(lastApplied+1)
			command := entry.Command
			index := entry.Index
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.mu.Lock()
			rf.DPrintf("committed index = %v", entry.Index)
			rf.lastApplied = max(rf.lastApplied, lastApplied+1)
			lastApplied = rf.lastApplied
			commitIndex = rf.commitIndex
			rf.DPrintf("applied entry = %v", entry)
			rf.mu.Unlock()
		}
	}
}

