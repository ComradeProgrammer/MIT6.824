package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

type KVServerSnapshot struct {
	KVMap     ShardMap
	LastIndex int
	//OpIDSet   map[int64]struct{}
	Config  shardctrler.Config

}
func NewKVServerSnapshot()KVServerSnapshot{
	return  KVServerSnapshot{}
}

func (kv *ShardKV) checkMaxSizeExceeded() {
	if kv.maxraftstate==-1 ||!kv.configApplied{
		return
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate/5*4 && kv.lastIndex != 0 {
		DPrintf("kvserver %d-%d start snapshop with lastIndex %d\n", kv.gid,kv.me, kv.lastIndex)
		//80% as the threshold, start snapshot
		snapShot := KVServerSnapshot{
			KVMap:     kv.kvMap.Copy(),
			LastIndex: kv.lastIndex,
			//OpIDSet:   kv.opIDSet,
			Config: kv.config.Copy(),
		}
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		encoder.Encode(snapShot)
		data := buffer.Bytes()
		kv.rf.Snapshot(kv.lastIndex, data)
		//DPrintf("kvserver %d-%d finish snapshop with lastIndex %d, log size %d\n", kv.gid,kv.me, kv.lastIndex)
	}
}

func (kv *ShardKV) handleSnapInstall(msg *raft.ApplyMsg) {
	DPrintf("kvserver %d-%d call condsnapshot %v\n",kv.gid, kv.me, msg)
	ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
	if ok {
		DPrintf("kvserver %d-%d receive ok in condsnapshot %v\n",kv.gid, kv.me, msg)
		//install the snapshot
		var snapshpot = NewKVServerSnapshot()
		buffer := bytes.NewBuffer(msg.Snapshot)
		decoder := labgob.NewDecoder(buffer)
		_ = decoder.Decode(&snapshpot)
		kv.lastIndex = snapshpot.LastIndex
		kv.kvMap = snapshpot.KVMap.Copy()
		kv.config=snapshpot.Config.Copy()
		kv.configApplied=true
		//kv.opIDSet = snapshpot.OpIDSet
	}else{
		DPrintf("kvserver %d-%d receive not ok in condsnapshot %v\n",kv.gid, kv.me, msg)
	}
}

func (kv *ShardKV) installSnapFromPersister() {
	DPrintf("kvserver %d-%d installSnapFromPersister\n",kv.gid,kv.me)
	var snapshpot KVServerSnapshot= NewKVServerSnapshot()
	data := kv.persister.ReadSnapshot()
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	_ = decoder.Decode(&snapshpot)
	if snapshpot.KVMap.Map==nil{
		kv.kvMap=NewShardMap()
		kv.lastIndex=0
		kv.config.Num=0
	}else{
		kv.lastIndex = snapshpot.LastIndex
		kv.kvMap = snapshpot.KVMap
		kv.config=snapshpot.Config.Copy()
		kv.configApplied=true
	}
	
	//kv.opIDSet = snapshpot.OpIDSet
}
