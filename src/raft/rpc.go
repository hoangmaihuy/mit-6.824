package raft

type AppendEntriesArgs struct {
	Term         int     // Leader's term
	LeaderId     int     // So follower can redirect clients
	PrevLogIndex int     // Index of log entry immediately preceding new ones
	PrevLogTerm  int     // Term of prevLogIndex entry
	Entries      []Entry // log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term                   int  // currentTerm, for leader to update itself
	Success                bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	IsLogConflict          bool
	ConflictTerm           int
	ConflictTermFirstIndex int
}

type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}
