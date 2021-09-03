package raft

const MaxAppendEntriesSize = 100

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTermL(args.Term)
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictTermFirstIndex = -1

	if args.Term < rf.currentTerm { // outdated leader
		reply.Success = false
		reply.IsLogConflict = false
	} else if prevEntry := rf.getLogEntry(args.PrevLogIndex); prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
		rf.setElectionTimeout()
		reply.Success = false
		reply.IsLogConflict = true
		// follower may be fallback far behind leader, so use last entry to quickly catch up
		if prevEntry == nil {
			prevEntry = rf.lastLogEntry()
		}
		if prevEntry != nil {
			reply.ConflictTerm = prevEntry.Term
			conflictIndex := prevEntry.Index
			for conflictEntry := rf.getLogEntry(conflictIndex); conflictIndex > rf.log.StartIndex && conflictEntry != nil && conflictEntry.Term == reply.ConflictTerm; {
				conflictIndex--
				conflictEntry = rf.getLogEntry(conflictIndex)
			}
			reply.ConflictTermFirstIndex = conflictIndex+1
		}
	} else {
		rf.setElectionTimeout()
		// not a heartbeat message
		if len(args.Entries) > 0 {
			rf.eraseEntries(args.PrevLogIndex + 1)
			rf.appendEntries(args.Entries...)
		}
		if args.LeaderCommit > rf.commitIndex {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogEntry().Index)
			if oldCommitIndex != rf.commitIndex {
				rf.applyCond.Broadcast()
			}
			rf.DPrintf("update commitIndex, old = %v, new = %v", oldCommitIndex, rf.commitIndex)
		}
		reply.Success = true
		reply.IsLogConflict = false
	}

	rf.DPrintf("AppendEntries args = %v, reply = %v", args, reply)
}

func (rf *Raft) sendAllAppendEntries(isHeartbeat bool) {
	rf.DPrintf("send all append entries, heartbeat = %v", isHeartbeat)
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, isHeartbeat)
		}
	}
}
func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool) {
	rf.mu.Lock()
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.log.StartIndex {
		rf.sendInstallSnapshotL(server)
	} else {
		rf.sendAppendEntriesL(server, nextIndex, isHeartbeat)
	}
}

func (rf *Raft) sendAppendEntriesL(server int, nextIndex int, isHeartbeat bool) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	rf.DPrintf("sendAppendEntriesL, log = %v, server = %v, isHeartbeat = %v, nextIndex = %v", rf.log.Entries, server, isHeartbeat, nextIndex)
	prevEntry := rf.getLogEntry(nextIndex - 1)
	var entries []Entry
	if isHeartbeat {
		entries = make([]Entry, 0)
	} else {
		entries = rf.getLogEntries(nextIndex, MaxAppendEntriesSize)
	}
	args = AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevEntry.Index,
		PrevLogTerm:  prevEntry.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		rf.setElectionTimeout()

		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.DPrintf("sendAppendEntriesL success, server = %v, nextIndex = %v", server, rf.nextIndex[server])
		} else {
			rf.updateTermL(reply.Term)
			if reply.IsLogConflict {
				if reply.ConflictTermFirstIndex > 0 && reply.ConflictTerm > 0 {
					rf.nextIndex[server] = reply.ConflictTermFirstIndex
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					rf.nextIndex[server] = args.PrevLogIndex
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
			}
		}
		rf.mu.Unlock()
	}
}
