package raft

import "fmt"

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//first,judge whether this snapshot should be accepted
	rf.Lock()
	defer rf.Unlock()
	if lastIncludedIndex<=rf.commitIndex{
		DPrintf("server %d abandoned old snapshot{lastIncludedTerm:%d, lastIncludedIndex:%d} with current state %s\n",
		rf.me,lastIncludedTerm,lastIncludedIndex-1,rf.String())
		return false
	}
	//install this snapshot
	rf.currentSnapShot=snapshot
	//log index from service starts from 1 
	rf.log.ReplaceLogs(make([]Log, 0),lastIncludedIndex-1,lastIncludedTerm,lastIncludedIndex)
	rf.lastApplied=lastIncludedIndex-1
	rf.commitIndex=lastIncludedIndex-1
	DPrintf("server %d installed snapshot{lastIncludedTerm:%d, lastIncludedIndex:%d} with current state %s\n",
		rf.me,lastIncludedTerm,lastIncludedIndex-1,rf.String())
	
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.Lock()
	defer rf.Unlock()
	DPrintf("server %d Snapshot is called, index is %d\n",rf.me,index-1)
	rf.currentSnapShot=snapshot
	rf.log.WipeOutSnapshottedLogs(index-1)
	rf.timerExpired=false
	rf.persist()

}

type InstallSnapShotArgs struct{
	Term int
	LeaderID int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

func (i InstallSnapShotArgs)String()string{
	return fmt.Sprintf("{Term:%d, LeaderID:%d, LastIncludedIndex:%d, LastIncludedTerm %d}",i.Term,i.LeaderID,i.LastIncludedIndex,i.LastIncludedTerm)
}

type InstallSnapShotReply struct{
	Term int
}

func(i InstallSnapShotReply)String()string{
	return fmt.Sprintf("{Term:%d}",i.Term)
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

//require lock
func  (rf *Raft) leaderInstallSnapShot(server int){
	args:=InstallSnapShotArgs{
		Term:rf.currentTerm,
		LeaderID: rf.me,
		LastIncludedIndex: rf.log.PrevIndex,
		LastIncludedTerm: rf.log.PrevLogTerm,
		Data:rf.currentSnapShot,
	}
	reply:=InstallSnapShotReply{}

	go func(i int){
		rf.sendInstallSnapShot(i,&args,&reply)
		rf.Lock()
		defer rf.Unlock()
		if rf.currentTerm!=args.Term{
			return
		}
		if rf.currentTerm<reply.Term{
			rf.switchToFollowerOfnewTerm(reply.Term)
			return
		}
		rf.nextIndex[i]=args.LastIncludedIndex+1
	}(server)
}

func(rf *Raft)InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply){
	//check term
	rf.Lock()
	defer rf.Unlock()
	DPrintf("server %d received InstallSnapshot %s with current state %s\n",rf.me,args,rf.String())
	if args.Term > rf.currentTerm {
		//if we find that we have fallen behind
		rf.switchToFollowerOfnewTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	
	msg:=ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		SnapshotTerm: args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex+1,
		Snapshot: args.Data,
	}
	rf.applyMsgQueue.Pushback(msg)
	DPrintf("server %d send Snapshot %s to applyCh with current state %s\n",rf.me,args,rf.String())
	return
}
