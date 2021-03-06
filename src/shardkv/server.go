package shardkv

import (
	"fmt"
	//"net/http"
	//_ "net/http/pprof"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)


// func init(){
// 	go func() {
// 		http.ListenAndServe("0.0.0.0:6060", nil)
// 	}()
// }
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	Nonce int64
	ConfigNum int
	Data interface{}
}
type OpResult struct{
	Err Err
	Value string
	Data interface{}
}

type ShardKV struct {
	sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister
	lastIndex int

	// Your definitions here.
	pendingChans map[int64][]chan OpResult
	nonces map[int64]struct{}
	shardNonces map[int]map[int64]struct{}
	kvMap ShardMap
	indexToNonce map[int]int64
	term int
	isLeader bool

	ctrlClient       *shardctrler.Clerk
	config shardctrler.Config
	newConfig shardctrler.Config
	configApplied bool
	us []string

	

	done chan struct{}
}

func (kv *ShardKV)String()string{
	if DEBUG{
		return fmt.Sprintf("{me:%d, gid:%d, config:%d configApplied:%v,isLeader:%v,"+
			"\n\tkvMap:%s\n\tshardnonce:\n}",kv.me,kv.gid,kv.config.Num,kv.configApplied,kv.isLeader,kv.kvMap.String()/*,kv.shardNonces*/)
	}
	return ""
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.done<-struct{}{}
	kv.done<-struct{}{}
	//kick out all hanging requests
	// kv.Lock()
	// defer kv.Unlock()
	// for _,chs:=range kv.pendingChans{
	// 	for _,ch:=range chs{
	// 		ch <- OpResult{Err:ErrWrongLeader}
	// 	}
	// }
	
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetShardsArgs{})
	labgob.Register(GetShardsReply{})
	labgob.Register(InstallShardArgs{})
	labgob.Register(InstallShardReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister=persister
	kv.lastIndex=0

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.pendingChans=make(map[int64][]chan OpResult)
	kv.indexToNonce=make(map[int]int64)
	kv.shardNonces=make(map[int]map[int64]struct{})
	kv.nonces=make(map[int64]struct{})
	kv.done=make(chan struct{},2)
	kv.term=0

	kv.ctrlClient= shardctrler.MakeClerk(ctrlers)
	kv.config.Num=0
	kv.configApplied=true

	kv.installSnapFromPersister()
	go kv.pullConfiguration()
	go kv.applyTicker()
	return kv
}
