package raft

import "time"

const TickerTimeout = time.Millisecond * 100

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently, also send heartbeat messages when is leader
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(TickerTimeout)

		rf.mu.Lock()
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		if time.Now().After(electionTimeout) {
			rf.startElection()
		} else {
			_, isLeader := rf.GetState()
			if isLeader {
				rf.sendAllAppendEntries(false)
				rf.updateCommit()
			}
		}
	}
}
