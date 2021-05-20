package raft

import (
	"bytes"

	"6.824/labgob"
)

type PersistentState struct{
	CurrentTerm int
	VotedFor int
	Logs []Log
	PrevIndex int
	PrevTerm int
	RealLength int

}	

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//require lock
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	var persistentState=PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor: rf.votedFor,
		Logs: rf.log.Logs,
		PrevIndex:rf.log.PrevIndex,
		PrevTerm: rf.log.PrevLogTerm,
		RealLength: rf.log.Len(),
	}
	DPrintf("server %d is persisting its state, current state %s", rf.me,rf.String())
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(persistentState)
	data:=buffer.Bytes()
	rf.persister.SaveRaftState(data)	
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
	var persistentState PersistentState
	buffer:=bytes.NewBuffer(data)
	decoder:=labgob.NewDecoder(buffer)
	err:=decoder.Decode(&persistentState)
	if err!=nil{
		DPrintf("server %d readPersist failed\n",rf.me)
		return
	}
	rf.currentTerm=persistentState.CurrentTerm
	rf.votedFor=persistentState.VotedFor
	rf.log.ReplaceLogs(persistentState.Logs,persistentState.PrevIndex,persistentState.PrevTerm,persistentState.RealLength)
}