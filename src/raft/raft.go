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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER  int = 0
	CANDIDATE int = 1
	LEADER    int = 2
)
const (
	HEARTBEATINTERVAL int = 100 // unit:ms should range
	ELECTIONINTERVAL  int = 300 //unit:ms should range from 300-400
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex                     // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	state      int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg //channel for upper level protocol to accept logs

	//data structure in figure 2
	currentTerm int
	votedFor    int //whenever we switch to a new term, set votedFor=-1
	log         LogVector
	commitIndex int
	lastApplied int
	nextIndex   []int //for leader only, initialized when become leader
	matchIndex  []int //for leader only

	//synchronization and other control messages

	//if we receive any message that we should reset timer, set this variable to false immediately
	timerExpired bool
	//used to count how many votes we get in an election
	votesGoted int
	//leaderAppendEntries depend on this cond variable to be woken up
	appendEntriesCond *sync.Cond
	currentSnapShot   []byte
	applyMsgQueue     *ThreadSafeQueue
}

/**
@brief convert the state of current raft module into string
@attention requires lock before being called
@attention this function will EXTREMELY affect the performance due to %v invokes marshal of raft opbject,
and if marshaling the raft object is involved with -race, it will be very time-consuming
make sure marshaling part of this function won't be called when your performance is being tested 
*/
func (rf *Raft) String() string {
	if DEBUG {
		var state string
		switch rf.state {
		case FOLLOWER:
			state = "follower"
		case CANDIDATE:
			state = "candidate"
		case LEADER:
			state = "leader"
		}
		return fmt.Sprintf("{\n\tme:%d, state:%s, currentTerm:%d, votedFor:%d, timerExpired:%v, votesGoted:%d, commitIndex:%d, lastApplied:%d,\n"+
			"\tlogs:%s,\n\tnextIndex:%v,\n\t matchIndex:%v\n\t}",
			rf.me, state, rf.currentTerm, rf.votedFor, rf.timerExpired, rf.votesGoted, rf.commitIndex, rf.lastApplied, rf.log.String(), rf.nextIndex, rf.matchIndex)
	} else {
		return ""
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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
//@attention will cause some changes to persistent state, thus persist() will be called
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.Lock()
	defer rf.Unlock()
	if rf.state == LEADER {
		term = rf.currentTerm
		isLeader = true
		index = rf.log.Len()
		newLog := Log{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		rf.log.PushBack(newLog)
		rf.appendEntriesCond.Broadcast()
		rf.persist()

	}
	if isLeader {
		DPrintf("server %d start command %v at pos %d with current state %s\n", rf.me, command, index, rf.String())
	} else {
		DPrintf("server %d rejected command %v with current state %s\n", rf.me, command, rf.String())
	}

	return index + 1, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/**
@brief goroutine for follower to check heartbeat periodically
@detail check whether we have received heartbeat in last ELECTIONINTERVAL period.
If not, start a election
if we win the election, this goroutine will be terminated

*/
func (rf *Raft) ticker() {

	rf.Lock()
	DPrintf("server %d started ticker\n", rf.me)
	rf.Unlock()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		ms := rand.Intn(100) + ELECTIONINTERVAL
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.Lock()
		if rf.timerExpired {
			rf.startElection()
			rf.Unlock()
			return

		}
		//if we didn't win as leader or timer didn't expire
		rf.timerExpired = true
		rf.Unlock()

	}

}

/**
@brief gorouine for leader to check whether there is any AppendEntriesRPC needs to be sent.
@detail scan the nextIndex for all server to find one that need to be send  AppendEntriesRPC request
if no such items is found, wait on rf.appendEntriesCond
*/
func (rf *Raft) leaderAppendEntriesTicker(term int) {
	pos := 0
	for !rf.killed() {
		rf.Lock()
		var server int

		//loop protecting the cond.Wait
		for {
			if rf.state != LEADER || rf.currentTerm != term {
				//stop at once if server is nolonger leader or switched to a new term
				rf.Unlock()
				return
			}
			// check whether it's time to append entries
			hasNext := false
			pos = pos % len(rf.peers)
			for ; pos < len(rf.peers); pos++ {
				if pos == rf.me {
					//don't check server itself
					continue
				}
				next := rf.nextIndex[pos]
				//new log has be found
				if rf.log.Len() > next {
					hasNext = true
					server = pos
					break
				}
			}
			if !hasNext {
				rf.appendEntriesCond.Wait()
			} else {
				break //go to append entries for server found
			}
		}
		//send appendentries for server found
		//this we won't use another goroutine and won't release lock until the reply is properly handled
		DPrintf("server %d find log should be appended for server %d with current state %s\n", rf.me, server, rf.String())
		pos++
		rf.leaderAppendEntries(server)

		rf.Unlock()
	}

}

/**
@brief goroutine for leader to send out heartbeat message
*/
func (rf *Raft) leaderTicker(term int) {
	for rf.killed() == false {
		rf.Lock()
		//send out heartbeat
		if rf.state != LEADER || rf.currentTerm != term {
			rf.Unlock()
			//non-leader nolonger need this tick
			return
		}
		for i := 0; i < len(rf.peers); i++ {

			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.Lock()
				if rf.state != LEADER || rf.currentTerm != term {
					rf.Unlock()
					return
				}
				rf.leaderAppendEntries(server)
				rf.Unlock()
			}(i)
		}
		//release the lock and sleep
		rf.Unlock()
		time.Sleep(time.Duration(HEARTBEATINTERVAL) * time.Millisecond)
	}

}

/**
@brief start a election.
@return whether we win the election
@attention require lock before being called
@attention will cause some changes to persistent state, thus persist() will be called
*/
func (rf *Raft) startElection() bool {
	for {
		//step0 convert to candidate
		rf.state = CANDIDATE
		//step 1 increment current term
		rf.currentTerm++
		var term = rf.currentTerm
		//then, vote for himself
		rf.votesGoted = 1
		rf.votedFor = rf.me
		rf.persist()
		DPrintf("server %d starts election with state%s\n", rf.me, rf.String())
		//3,set up election timer
		ms := ELECTIONINTERVAL + rand.Intn(100)
		//4. ask for votes from all peers
		lastLogIndex, lastLogTerm := rf.log.GetLastIndexAndterm()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			var arg = RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			go func(server int) {
				var reply = RequestVoteReply{}
				ok := rf.sendRequestVote(server, &arg, &reply)
				if !ok {
					//the peer is down
					return
				}
				rf.Lock()
				defer rf.Unlock()
				//if the reply contains a higher term, switch to new term,switch to follower
				if reply.Term > rf.currentTerm {
					rf.switchToFollowerOfnewTerm(reply.Term)

				} else if rf.currentTerm == reply.Term && reply.VoteGranted {
					//else, check vote
					rf.votesGoted++
				}
			}(i)
		}
		rf.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.Lock()
		if term == rf.currentTerm && rf.state == CANDIDATE {
			//Nothing wrong happened, election wasn't stopped
			//check the vote numbers wo got
			DPrintf("server %d get %d votes in election for term%d \n", rf.me, rf.votesGoted, term)
			if rf.votesGoted >= len(rf.peers)/2+1 {
				//we won the election
				rf.switchToLeader()
				return true
			} else {
				//split vote or brain split,continue for next election
				continue
			}

		} else {
			//election was stopped
			return false
		}
	}

}

/**
@brief apply Log to upper level protocol
@attention require lock before being called
*/
func (rf *Raft) applyLog(index int) {

	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log.Get(index).Command,
		CommandIndex: index + 1,
	}
	DPrintf("server %d is trying to apply log %v\n", rf.me, applyMsg)
	rf.applyMsgQueue.Pushback(applyMsg)

}
func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		m := rf.applyMsgQueue.PopFront()
		rf.applyCh <- m
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
	rf.applyCh = applyCh

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = NewLogVector()
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.timerExpired = true
	rf.appendEntriesCond = sync.NewCond(&rf.Mutex)
	rf.currentSnapShot = nil
	rf.applyMsgQueue = NewThreadSafeQueue()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyTicker()

	return rf
}
