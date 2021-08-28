package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// for whatever reasons, if currentTerm is out-of-date then back to Follower
// should hold the lock before calling this
func (rf *Raft) updateTerm(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		if rf.state == Leader { // outdated leader back online
			rf.votedFor = -1
		}
		rf.state = Follower
	}
}

// reset electionTimeout to a random duration
func (rf *Raft) resetElectionTimeout() {
	timeout := rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

func (rf *Raft) lastEntry() *Entry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getEntry(index int) *Entry {
	if index > len(rf.logs)-1 {
		return nil
	}
	return &rf.logs[index]
}

func (rf *Raft) getEntries(fromIndex int, size int) []Entry {
	toIndex := min(len(rf.logs), fromIndex+size)
	return rf.logs[fromIndex : toIndex]
}

func (rf *Raft) eraseEntries(fromIndex int) {
	rf.logs = rf.logs[:fromIndex]
}

func (rf *Raft) appendEntries(newEntries []Entry) {
	rf.logs = append(rf.logs, newEntries...)
}

func compareTermAndIndex(term1 int, index1 int, term2 int, index2 int) int {
	switch {
	case term1 < term2:
		return -1
	case term1 > term2:
		return 1
	case index1 < index2:
		return -1
	case index1 > index2:
		return 1
	default:
		return 0
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
