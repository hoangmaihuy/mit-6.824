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
			{
				Index:   0,
				Term:    0,
				Command: nil,
			},
		},
	}
}

func (rf *Raft) getLogLen() int {
	return len(rf.log.Entries)
}

func (rf *Raft) lastLogEntry() *Entry {
	return &rf.log.Entries[len(rf.log.Entries)-1]
}

func (rf *Raft) getLogEntry(index int) *Entry {
	//DPrintf("raft %v LoggetLogEntry: index = %v", rf.me, index)
	index -= rf.log.StartIndex
	if index >= len(rf.log.Entries) || index < 0 {
		return nil
	}
	return &rf.log.Entries[index]
}

func (rf *Raft) getLogEntries(fromIndex int, size int) []Entry {
	//fmt.Printf("getLogEntries: [%v, %v]\n", fromIndex, size)
	fromIndex -= rf.log.StartIndex
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
	fromIndex -= rf.log.StartIndex
	rf.log.Entries = rf.log.Entries[:fromIndex]
}

func (rf *Raft) appendEntries(newEntries ...Entry) {
	entries := make([]Entry, len(newEntries))
	copy(entries, newEntries)
	rf.log.Entries = append(rf.log.Entries, entries...)
	rf.persist()
}
