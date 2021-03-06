package raft

/**
	@brief change to the follower state and new term
	@attention require lock before being called 
	@param term new term number
	@detail will change to new term, follower state, wipe out data that only belong to leader,and start follower ticker
	@attention will cause some changes to persistent state, thus persist() will be called
*/
func (rf *Raft) switchToFollowerOfnewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.matchIndex = nil
	rf.nextIndex = nil
	rf.persist()
	if rf.state != FOLLOWER {
		DPrintf("server %d switch to follower\n", rf.me)
		rf.state = FOLLOWER
		go rf.ticker()
	}
}

/**
	@brief change to the leader state 
	@attention require lock before being called 
	@detail will change to leader state , reconstruct leader data structure and start leaderTicker,leaderAppendEntriesTicker
*/
func (rf *Raft) switchToLeader() {
	if rf.state != LEADER {
		rf.state = LEADER
		DPrintf("server %d switch to leader\n", rf.me)
		//init two arrays
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.log.Len()
			rf.matchIndex[i] = -1
		}
		go rf.leaderTicker(rf.currentTerm)
		go rf.leaderAppendEntriesTicker(rf.currentTerm)
	}

}


/**
	@brief change commitIndex and maintain related rules
	@attention require lock before being called 
	@see No.1 rule for all servers in Figure 2
*/
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

/**
	@brief change MatchIndex for a certain server and maintain related rules
	@attention require lock before being called 
	@param server index of server
	@param new value for rf.matchIndex[server]
	@see No.1 rule for all servers in Figure 2
*/
func (rf *Raft) updateMatchIndex(server, value int) {
	rf.matchIndex[server] = value
	DPrintf("server %d setMatchIndex[%d] as %d\n", rf.me, server, value)
	//scan for the log needs to be applied
	for i := rf.log.Len() - 1; i >= rf.commitIndex+1; i-- {
		if rf.log.Get(i).Term < rf.currentTerm {
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
