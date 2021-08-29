package raft

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type Log struct {
	Entries    []Entry
	StartIndex int
}

func makeLog() Log {
	return Log{
		StartIndex: 0,
		Entries: []Entry{
			{Index: 0, Term: 0, Command: nil},
		},
	}
}
// get last log entry, assume that there is always one dummy entry with index = 0
func (rf *Raft) lastEntry() *Entry {
	return rf.getEntry(len(rf.log.Entries)-1)
}

func (rf *Raft) getEntry(index int) *Entry {
	//DPrintf("raft %v getEntry: index = %v", rf.me, index)
	if index >= len(rf.log.Entries) {
		return nil
	}
	return &rf.log.Entries[index]
}

func (rf *Raft) getEntries(fromIndex int, size int) []Entry {
	//fmt.Printf("getEntries: [%v, %v]\n", fromIndex, size)
	if fromIndex >= len(rf.log.Entries) {
		return make([]Entry, 0)
	}
	toIndex := min(len(rf.log.Entries), fromIndex+size)
	entries := make([]Entry, toIndex - fromIndex)
	copy(entries, rf.log.Entries[fromIndex:toIndex])
	return entries
}

// use when log Entries conflict
func (rf *Raft) eraseEntries(fromIndex int) {
	rf.log.Entries = rf.log.Entries[:fromIndex]
	rf.persist()
}

func (rf *Raft) appendEntries(newEntries ...Entry) {
	rf.log.Entries = append(rf.log.Entries, newEntries...)
	rf.persist()
}
