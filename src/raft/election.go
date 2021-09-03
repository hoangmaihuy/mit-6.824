package raft

import (
	"math/rand"
	"time"
)

const MinElectionTimeout = 500 // milliseconds
const MaxElectionTimeout = 1000 // milliseconds

// RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTermL(args.Term)

	reply.Term = rf.currentTerm
	lastEntry := rf.lastLogEntry()
	isUpToDate := (lastEntry.Term < args.LastLogTerm) || (lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex)
	// current raft's term is more up-to-date so reject candidate's election
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 && isUpToDate) || rf.votedFor == args.CandidateId {
		// haven't voted for other candidates or voted for this candidate
		rf.setElectionTimeout()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		rf.setElectionTimeout()
		reply.VoteGranted = false
	}
	rf.DPrintf("RequestVote, log = %v, args = %v, reply = %v", rf.log, args, reply)
}

// set electionTimeout to a random duration
func (rf *Raft) setElectionTimeout() {
	//rf.DPrintf("set new election timeout")
	ms := rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
	rf.electionTimeout = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()

	rf.state = Candidate
	rf.setElectionTimeout()

	rf.DPrintf("startElection")
	rf.sendAllRequestVotesL()
}

func (rf *Raft) sendAllRequestVotesL() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogEntry().Index,
		LastLogTerm:  rf.lastLogEntry().Term,
	}
	voteCount := 1
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &voteCount)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.setElectionTimeout()
		if reply.VoteGranted {
			*voteCount++
			if *voteCount == len(rf.peers)/2+1 && rf.currentTerm == args.Term {
				rf.becomeLeaderL()
				rf.sendAllAppendEntries(true)
			}
		} else {
			rf.updateTermL(reply.Term)
		}
	}
}

func (rf *Raft) becomeLeaderL() {
	rf.DPrintf("become new leader")
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogEntry().Index + 1
	}
}
