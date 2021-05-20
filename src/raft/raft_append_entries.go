package raft

import (
	"encoding/json"
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

func (r AppendEntriesArgs) String() string {
	return fmt.Sprintf("{term:%d, leaderID:%d, prevLogIndex:%d, prevLogTerm:%d, entries:%d logs, leaderCommit:%d}",
		r.Term, r.LeaderID, r.PrevLogIndex, r.PrevLogTerm, len(r.Entries), r.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//for faster rollback
	XTerm int
	XIndex int
	XLen int
}

func (r AppendEntriesReply) String() string {
	data, _ := json.Marshal(r)
	return string(data)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/**
	@brief handler of AppendRntries RPC
	@attention this function can change the current state
	@attention will cause some changes to persistent state, thus persist() will be called
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Code of AppendRntries RPC
	rf.Lock()
	defer rf.Unlock()
	DPrintf("server %d recieved AppendEntries %s with current state %s\n", rf.me, args, rf.String())

	if args.Term < rf.currentTerm {
		//if the server requesting vote falls behind,reject
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("server %d respondAppendEntries %s with %s \n", rf.me, args, reply)
		return
	} else if args.Term > rf.currentTerm {
		//if we find that we have fallen behind
		rf.switchToFollowerOfnewTerm(args.Term)
	}
	rf.timerExpired = false
	reply.Term = rf.currentTerm
	//check whether there is a log matches the prevLogIndex
	reply.Success = true
	reply.XLen=rf.log.Len()
	reply.XTerm=-1
	reply.XIndex=-1
	//if args.PrevLogIndex is -1, that means the whole log needs to be replaced,so just accept all, no need to check the inconsistency
	if args.PrevLogIndex != -1 {
		//not trying to overwrite the first log
		if args.PrevLogIndex >=rf.log.Len() {
			//reject if our log is too short
			reply.Success = false
		} else if rf.log.Get(args.PrevLogIndex).Term != args.PrevLogTerm {
			//reject and set XTerm and XIndex
			//XTerm is the term of conflicting entry
			//XIndex is the first entry of the conflicting term in follower
			reply.XTerm= rf.log.Get(args.PrevLogIndex).Term
			index:=args.PrevLogIndex
			for ;index>=0&&rf.log.Get(index).Term==rf.log.Get(args.PrevLogIndex).Term;index--{}
			reply.XIndex=index+1	
			reply.Success = false
		}

	}
	if reply.Success {
		//now we should accept the append entry request
		//rf.log = rf.log[:args.PrevLogIndex+1]
		//rf.log = append(rf.log, args.Entries...)
		rf.log.WipeOutInconsistentLogs(args.PrevLogIndex)
		rf.log.PushBack(args.Entries...)
		//check leader commit
		newIndex := args.LeaderCommit
		if rf.log.Len()-1 < newIndex {
			newIndex =rf.log.Len() - 1
		}
		if newIndex > rf.commitIndex {
			//go to commit the newest log it has
			rf.updateCommitIndex(newIndex)
		}
		rf.persist()
		
	}

	DPrintf("server %d respondAppendEntries %s with %s,current state %s\n", rf.me, args, reply, rf.String())

}

/**
	@brief leader will call this function to send  AppendEntries 
	@detail this function may be called in leaderAppendEntriesTicker, if leader detect that a follower has fallen behind in log for current term
	this function may also be called in leaderTicker to send a heartbeat message.
	even we send a heartbeat message with no entries attached, we also need to fill all fields correctly according to nextIndex array,
	for follower cannot distinguish wherther this is a heartbeat or not.

	if the message is a heartbeat, the entries will be nil, and the PrevLogIndex and PrevLogTerm will be set according to the last log we contain
	when handle the reply, we use faster rollback mentioned in class

	@attention this function can change the current state
	@attention require lock
*/
func (rf *Raft) leaderAppendEntries(server int) {

	//first, according to whether we should use appendentries or snapshot?
	if rf.log.PrevIndex!=-1&& rf.nextIndex[server]<=rf.log.PrevIndex{
		//we should user installsnapshot
		rf.leaderInstallSnapShot(server)
		return
	}
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  -1,
		LeaderCommit: rf.commitIndex,
	}
	if rf.nextIndex[server]-1 >= 0 {
		arg.PrevLogTerm = rf.log.Get(rf.nextIndex[server]-1).Term
	}
	length := rf.log.Len() - rf.nextIndex[server]
	arg.Entries = make([]Log, length)
	for i := 0; i < length; i++ {
		arg.Entries[i] = rf.log.Get(rf.nextIndex[server]+i)
	}
	reply := AppendEntriesReply{}
	DPrintf("server %d sendAppendEntries %s\n", rf.me, arg)
	//send the request

	finishChan := make(chan bool)
	expireChan := time.After(time.Duration(HEARTBEATINTERVAL) * time.Millisecond)
	go func() {
		finishChan <- rf.sendAppendEntries(server, &arg, &reply)

	}()
	term:=rf.currentTerm
	rf.Unlock()
	select {
	case <-expireChan:
		rf.Lock()
		if rf.state!=LEADER|| rf.currentTerm!=term{
			rf.switchToFollowerOfnewTerm(rf.currentTerm)
		}
		DPrintf("server %d sendAppendEntries to %d failed for time out, current pos\n", rf.me, server)
	case ok := <-finishChan:
		rf.Lock()
		if rf.state!=LEADER|| rf.currentTerm!=term{
			rf.switchToFollowerOfnewTerm(rf.currentTerm)
			return
		}
		if !ok {
			DPrintf("server %d sendAppendEntries to %d failed for return false, current pos\n", rf.me, server)
		} else {
			//if success,
			if reply.Success {
				rf.nextIndex[server] = arg.PrevLogIndex+len(arg.Entries)+1
				rf.updateMatchIndex(server, arg.PrevLogIndex+len(arg.Entries))
			} else if reply.Term > rf.currentTerm {
				//switch to follower
				rf.switchToFollowerOfnewTerm(reply.Term)
				return
			} else {
				//use faster rollback
				//rf.nextIndex[server]--

				if reply.XLen<=arg.PrevLogIndex{
					//case 3,the follower just don't have so many logs
					rf.nextIndex[server]=reply.XLen
				}else{
					//check whether leader have the term in XTerm
					var index=arg.PrevLogIndex
					for;index>=0&&rf.log.Get(index).Term!=reply.XTerm;index--{}
					if index==-1{
						//no such term is found
						rf.nextIndex[server]=reply.XIndex
					}else{
						//find out the start of that term
						for;index>=0&&rf.log.Get(index).Term==reply.XTerm;index--{}
						rf.nextIndex[server]=index+1
					}
				}
			}
		}
	}

}

