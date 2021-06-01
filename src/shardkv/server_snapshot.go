package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
)

type KVServerSnapshot struct {
	KVMap     ShardMap
	LastIndex int
	//OpIDSet   map[int64]struct{}
}
func NewKVServerSnapshot()KVServerSnapshot{
	return  KVServerSnapshot{}
}

func (kv *ShardKV) checkMaxSizeExceeded() {
	if kv.maxraftstate==-1{
		return
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate/5*4 && kv.lastIndex != 0 {
		DPrintf("kvserver %d start snapshop with lastIndex %d\n", kv.me, kv.lastIndex)
		//80% as the threshold, start snapshot
		snapShot := KVServerSnapshot{
			KVMap:     kv.kvMap.Copy(),
			LastIndex: kv.lastIndex,
			//OpIDSet:   kv.opIDSet,
		}
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		encoder.Encode(snapShot)
		data := buffer.Bytes()
		kv.rf.Snapshot(kv.lastIndex, data)
	}
}

func (kv *ShardKV) handleSnapInstall(msg *raft.ApplyMsg) {
	DPrintf("kvserver %d call condsnapshot %v\n", kv.me, msg)
	ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
	if ok {
		//install the snapshot
		var snapshpot = NewKVServerSnapshot()
		buffer := bytes.NewBuffer(msg.Snapshot)
		decoder := labgob.NewDecoder(buffer)
		_ = decoder.Decode(&snapshpot)
		kv.lastIndex = snapshpot.LastIndex
		kv.kvMap = snapshpot.KVMap
		//kv.opIDSet = snapshpot.OpIDSet
	}
}

func (kv *ShardKV) installSnapFromPersister() {
	var snapshpot KVServerSnapshot= NewKVServerSnapshot()
	data := kv.persister.ReadSnapshot()
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	_ = decoder.Decode(&snapshpot)
	if snapshpot.KVMap.Map==nil{
		kv.kvMap=NewShardMap()
		kv.lastIndex=0
	}else{
		kv.lastIndex = snapshpot.LastIndex
		kv.kvMap = snapshpot.KVMap
	}
	
	//kv.opIDSet = snapshpot.OpIDSet
}
