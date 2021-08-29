package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{})  {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) DPrintf(format string, a ...interface{})  {
	if Debug {
		params := append([]interface{}{rf.me, rf.currentTerm}, a...)
		log.Printf("[raft %v, term %v] : " + format, params...)
	}
}



func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
