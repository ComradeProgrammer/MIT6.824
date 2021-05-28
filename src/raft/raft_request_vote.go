package raft

import (
	"encoding/json"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (r RequestVoteArgs) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r RequestVoteReply) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/**
@brief RPC Handler for "raft.RequestVote"
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	DPrintf("server %d recieved RequestVote %s with current state %s\n", rf.me, args, rf.String())

	if args.Term < rf.currentTerm {
		//if the server requesting vote falls behind,reject
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("server %d respond RequestVote %s with %s,with current state %s\n", rf.me, args, reply, rf.String())
		return
	} else if args.Term > rf.currentTerm {
		//if we find that we have fallen behind
		rf.switchToFollowerOfnewTerm(args.Term)
		reply.Term = rf.currentTerm

	}
	/*check whether they are  more up-to-date
	If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date*/

	moreUpToDate := true //whether they are more up-to-date
	lastLogIndex, lastLogTerm := rf.log.GetLastIndexAndterm()
	if args.LastLogTerm < lastLogTerm {
		moreUpToDate = false
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex < lastLogIndex {
			moreUpToDate = false
		}
	}
	//check whether i have voted for another peer
	if rf.votedFor == -1 && moreUpToDate {
		//haven't yet
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.timerExpired = false
		rf.persist()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	DPrintf("server %d respond RequestVote %s with %s,with current state %s\n", rf.me, args, reply, rf.String())
	return

}
