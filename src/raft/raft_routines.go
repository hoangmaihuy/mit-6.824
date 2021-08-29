package raft

import (
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		time.Sleep(rf.electionTimeout)
		//DPrintf("ticker on raft %v", rf.me)

		rf.mu.Lock()

		if rf.state != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			// start an election
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.persist()
			DPrintf("raft %v, term = %v, started an election", rf.me, rf.currentTerm)
			rf.state = Candidate
			rf.lastHeartbeat = time.Now()
			rf.resetElectionTimeout()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastEntry().Index,
				LastLogTerm:  rf.lastEntry().Term,
			}
			rf.mu.Unlock()

			voteCount := 0
			for i := range rf.peers {
				go func(server int) {
					reply := RequestVoteReply{}
					if rf.sendRequestVote(server, &args, &reply) {
						rf.mu.Lock()
						if reply.VoteGranted {
							voteCount++
						}
						rf.updateTerm(reply.Term)
						if voteCount > len(rf.peers)/2 && args.Term == rf.currentTerm { // win majority
							DPrintf("raft %v won election with voteCount = %v", rf.me, voteCount)
							rf.state = Leader
							rf.sendEntries()
						}
						rf.mu.Unlock()
					}
				}(i)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) commit(toIndex int) {
	DPrintf("raft %v commit toIndex = %v", rf.me, toIndex)
	for i := rf.commitIndex + 1; i <= toIndex; i++ {
		entry := rf.getEntry(i)
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.commitIndex = i
	}
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
			rf.commit(i)
		}
	}
}

// heartbeat is a go routine for Leader to send AppendEntries message
func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				rf.nextIndex[i] = min(rf.lastEntry().Index+1, rf.nextIndex[i])
				nextIndex := rf.nextIndex[i]
				//fmt.Printf("leader %v sendEntries to raft %v, nextIndex = %v\n", rf.me, i, nextIndex)
				nextEntries := rf.getEntries(nextIndex, 10)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getEntry(nextIndex - 1).Index,
					PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
					Entries:      nextEntries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					rf.updateTerm(reply.Term)
					if rf.currentTerm == args.Term {
						if reply.Success {
							rf.nextIndex[i] = nextIndex + len(nextEntries)
							rf.matchIndex[i] = rf.nextIndex[i] - 1
						} else if rf.nextIndex[i] > 1 {
							rf.nextIndex[i] -= len(nextEntries)
							if rf.nextIndex[i] <= 1 {
								rf.nextIndex[i] = 1
							}
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(HeartbeatInterval)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		//DPrintf("heartbeat on raft %v, term = %v, state = %v", rf.me, term, rf.state)
		if state == Leader {
			rf.updateCommit()
			rf.sendEntries()
		}
	}
}

func (rf *Raft) applyEntries() {
	time.Sleep(100 * time.Millisecond)
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for ; rf.lastApplied < commitIndex; rf.lastApplied++ {
		//entry := rf.getEntry(rf.lastApplied)
		// apply command
		continue

	}
}
