package raft

//dependence: require lock first
func (rf *Raft) switchToFollowerOfnewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.matchIndex = nil
	rf.nextIndex = nil
	if rf.state != FOLLOWER {
		DPrintf("server %d switch to follower\n", rf.me)
		rf.state = FOLLOWER
		go rf.ticker()
	}
}

func (rf *Raft) switchToLeader() {
	if rf.state != LEADER {
		rf.state = LEADER
		DPrintf("server %d switch to leader\n", rf.me)
		//init two arrays
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = -1
		}
		go rf.leaderTicker(rf.currentTerm)
		go rf.leaderAppendEntriesTicker(rf.currentTerm)
	}

}

//dependence: require lock first
//No.1 rule for all servers in Figure 2
func (rf *Raft) updateCommitIndex(commitIndex int) {
	DPrintf("server %d set commitIndex as %d and try to apply all the logs before it with state%s\n", rf.me, commitIndex, rf.String())
	rf.commitIndex = commitIndex
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyLog(i)
		}
	}
	rf.lastApplied = rf.commitIndex

}

//dependence: require lock first
//No.4 rule for all servers in Figure 2
func (rf *Raft) updateMatchIndex(server, value int) {
	rf.matchIndex[server] = value
	DPrintf("server %d setMatchIndex[%d] as %d\n", rf.me, server, value)
	//scan for the log needs to be applied
	for i := len(rf.log) - 1; i >= rf.commitIndex+1; i-- {
		if rf.log[i].Term < rf.currentTerm {
			continue
		}
		//check whether this log has been replicated into a majority servers
		count := 1 // count myself
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.updateCommitIndex(i)
			break
		}
	}
}
