package shardctrler

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JOIN  string = "join"
	LEAVE string = "leave"
	MOVE  string = "move"
	QUERY string = "query"
)

type ShardCtrler struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	nonces map[int64]struct{}
	hangingChannels map[int64][]chan OpResult
	indexToNonce map[int]int64

	terminate chan struct{}
	term int

	gids []int//the list of gids, sorted
	gidToServers map[int][]string
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	//terminate applyTicker goroutine
	sc.terminate<-struct{}{}
	//kick out all hanging requests
	sc.Lock()
	defer sc.Unlock()
	for _,chs:=range sc.hangingChannels{
		for _,ch:=range chs{
			ch<-OpResult{Err: KILLED,Success: false}
		}
	}

}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.nonces=make(map[int64]struct{})
	sc.hangingChannels=make(map[int64][]chan OpResult)
	sc.indexToNonce=make(map[int]int64)
	sc.terminate=make(chan struct{})
	sc.term=0
	sc.gidToServers=make(map[int][]string)

	go sc.applyTicker()
	return sc
}
