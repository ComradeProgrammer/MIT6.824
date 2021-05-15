package raft

import (
	"encoding/json"
	"fmt"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int

	LeaderCommit int
}

func (r AppendEntriesArgs) String() string {
	data, _ := json.Marshal(r)
	return string(data)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r AppendEntriesReply) String() string {
	data, _ := json.Marshal(r)
	return string(data)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//handler of AppendRntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Code of AppendRntries RPC
	rf.Lock()
	defer rf.Unlock()
	if DEBUG {
		fmt.Printf("server %d recieved AppendEntries %s with current state %s\n", rf.me, args, rf.String())
	}
	if args.Term < rf.currentTerm {
		//if the server requesting vote falls behind,reject
		reply.Term = rf.currentTerm
		reply.Success = false
		if DEBUG {
			fmt.Printf("server %d respondAppendEntries %s with %s ", rf.me, args, reply)
		}
		return
	} else if reply.Term > rf.currentTerm {
		//if we find that we have fallen behind
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
			go rf.ticker()
		}
		if DEBUG {
			fmt.Printf("server %d switch to follower\n", rf.me)
		}
	}
	rf.timerExpired = false
	reply.Term = rf.currentTerm
	//todo: continue to process log

	if DEBUG {
		fmt.Printf("server %d respondAppendEntries %s with %s\n", rf.me, args, reply)
	}

}
