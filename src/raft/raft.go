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
	HEARTBEATINTERVAL int  = 200 // unit:ms should range
	ELECTIONINTERVAL  int  = 300 //unit:ms should range from 300-500
	DEBUG             bool = false
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

	//data structure in figure 2
	currentTerm int
	votedFor    int //whenever we switch to a new term, set votedFor=-1

	//synchronization and other control messages
	timerExpired bool
	votesGoted   int
}

//should be used when lock have been retrived
func (rf *Raft) String() string {
	var state string
	switch rf.state {
	case FOLLOWER:
		state = "follower"
	case CANDIDATE:
		state = "candidate"
	case LEADER:
		state = "leader"
	}
	return fmt.Sprintf("{me:%d, state:%s, currentTerm:%d, votedFor:%d, timerExpired:%v, votesGoted:%d}",
		rf.me, state, rf.currentTerm, rf.votedFor, rf.timerExpired, rf.votesGoted)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	if DEBUG {
		rf.Lock()
		fmt.Printf("server %d started ticker\n", rf.me)
		rf.Unlock()
	}
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		ms := rand.Intn(200) + ELECTIONINTERVAL
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

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		rf.Lock()
		//send out heartbeat
		if rf.state != LEADER {
			rf.Unlock()
			//non-leader nolonger need this tick
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			arg := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me,
				//todo: add more information here
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &arg, &reply)
				if !ok {
					return
				}
				rf.Lock()
				defer rf.Unlock()
				if reply.Term > rf.currentTerm {
					//we find we have fallen behind
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					if rf.state != FOLLOWER {
						rf.state = FOLLOWER
						go rf.ticker()
					}

				}
			}(i)
		}
		//release the lock and sleep
		rf.Unlock()
		time.Sleep(time.Duration(HEARTBEATINTERVAL) * time.Millisecond)
	}

}

//lock should be retrived before call this function
//lock should be retrived when return from this function
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
		if DEBUG {
			fmt.Printf("server %d starts election with state%s\n", rf.me, rf.String())
		}
		//3,set up election timer
		ms := ELECTIONINTERVAL + rand.Intn(200)
		//4. ask for votes from all peers
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			var arg = RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: 0, // todo: modify here
				LastLogTerm:  0, //todo: modify for next lab
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
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					if DEBUG {
						fmt.Printf("server %d switch to follower\n", rf.me)
					}
					go rf.ticker()

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
			if DEBUG {
				fmt.Printf("server %d get %d votes in election for term%d \n", rf.me, rf.votesGoted, term)
			}
			if rf.votesGoted >= len(rf.peers)/2+1 {
				//we won the election
				rf.state = LEADER
				if DEBUG {
					fmt.Printf("server %d switch to leader\n", rf.me)
				}
				go rf.leaderTicker()
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
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = FOLLOWER

	rf.timerExpired = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
