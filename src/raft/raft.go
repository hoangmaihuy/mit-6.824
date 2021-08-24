package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
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

const HeartbeatInterval = time.Millisecond * 100

const MinElectionTimeout = 500 // milliseconds
const MaxElectionTimeout = 1000 // milliseconds

type RaftState int

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

type Entry struct {
	Term int // Term when entry was received by leader
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int           // Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor        int           // CandidateId that received vote in current term (or -1 if none)
	state           RaftState     // Current state of server
	electionTimeout time.Duration // After electionTimeout without receiving heartbeat message, start election
	lastHeartbeat   time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)

	reply.Term = rf.currentTerm
	switch {
	// current raft's term is more up-to-date so reject candidate's election
	case args.Term < rf.currentTerm:
		reply.VoteGranted = false
	// haven't voted for other candidates or voted for this candidate
	case rf.votedFor == -1 || rf.votedFor == args.CandidateId:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	default:
		reply.VoteGranted = false
	}
	//DPrintf("RequestVote: args = %v, reply = %v", args, reply)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC
//
type AppendEntriesArgs struct {
	Term         int     // Leader's term
	LeaderId     int     // So follower can redirect clients
	PrevLogIndex int     // Index of log entry immediately preceding new ones
	PrevLogTerm  int     // Term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	switch {
	case args.Term < rf.currentTerm: // outdated leader
		reply.Success = false
	default:
		rf.lastHeartbeat = time.Now()
		reply.Success = true
	}

	//DPrintf("AppendEntries: args = %v, reply = %v", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("raft %v is killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		time.Sleep(rf.electionTimeout)
		//DPrintf("ticker on raft %v", rf.me)

		rf.mu.Lock()

		if rf.state == Follower && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			// start an election
			//DPrintf("raft %v started an election", rf.me)
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.state = Candidate
			rf.lastHeartbeat = time.Now()
			rf.resetElectionTimeout()
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: 0,
				LastLogTerm: 0,
			}
			rf.mu.Unlock()

			mutex := sync.Mutex{}
			voteCount := 0
			for i := range rf.peers {
				go func(server int) {
					reply := RequestVoteReply{}
					if rf.sendRequestVote(server, &args, &reply) {
						if reply.VoteGranted {
							mutex.Lock()
							voteCount++
							mutex.Unlock()
						}
					}
				}(i)
			}

			// wait for other servers to vote
			time.Sleep(time.Second)

			rf.mu.Lock()
			mutex.Lock()
			if voteCount > len(rf.peers) / 2 { // win majority
				DPrintf("raft %v won election with voteCount = %v", rf.me, voteCount)
				rf.state = Leader
				rf.sendHeartbeat()
			} else {
				DPrintf("raft %v lost election with voteCount = %v", rf.me, voteCount)
				rf.state = Follower
				rf.votedFor = -1
			}
			mutex.Unlock()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

// heartbeat is a go routine for Leader to send empty AppendEntries message
func (rf *Raft) sendHeartbeat() {
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: 0,
		PrevLogTerm: 0,
		Entries: nil,
		LeaderCommit: 0,
	}
	for i := range rf.peers {
		go rf.sendAppendEntries(i, &args, new(AppendEntriesReply))
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(HeartbeatInterval)
		rf.mu.Lock()
		//DPrintf("heartbeat on raft %v, term = %v, state = %v", rf.me, term, rf.state)
		if rf.state == Leader {
			rf.sendHeartbeat()
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeat goroutine to send heartbeat messages
	go rf.heartbeat()
	return rf
}
