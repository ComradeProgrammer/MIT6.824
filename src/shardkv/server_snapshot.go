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
	ShardNonce map[int]map[int64]struct{}
	Us []string

}
func NewKVServerSnapshot()KVServerSnapshot{
	return  KVServerSnapshot{}
}

func (kv *ShardKV) checkMaxSizeExceeded() {
	if kv.maxraftstate==-1 ||!kv.configApplied{
		return
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate*2 && kv.lastIndex != 0 {
		DPrintf("kvserver %d-%d start snapshop with lastIndex %d\n", kv.gid,kv.me, kv.lastIndex)
		//80% as the threshold, start snapshot
		snapShot := KVServerSnapshot{
			KVMap:     kv.kvMap.Copy(),
			LastIndex: kv.lastIndex,
			//OpIDSet:   kv.opIDSet,
			Config: kv.config.Copy(),
			ShardNonce: kv.copyShardNonce(),
			Us:kv.us,
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
		kv.us=snapshpot.Us
		kv.shardNonces=copyShardNonce2( snapshpot.ShardNonce)
		//kv.opIDSet = snapshpot.OpIDSet
	}else{
		DPrintf("kvserver %d-%d receive not ok in condsnapshot %v\n",kv.gid, kv.me, msg)
	}
}

func(kv *ShardKV)copyShardNonce()map[int]map[int64]struct{}{
	var res=make(map[int]map[int64]struct{})
	for k,v:=range kv.shardNonces{
		res[k]=make(map[int64]struct{})
		for k2,v2:=range v{
			res[k][k2]=v2
		}
	}
	return res
}
func copyShardNonce2(src map[int]map[int64]struct{})map[int]map[int64]struct{}{
	var res=make(map[int]map[int64]struct{})
	for k,v:=range src{
		res[k]=make(map[int64]struct{})
		for k2,v2:=range v{
			res[k][k2]=v2
		}
	}
	return res
}


func (kv *ShardKV) installSnapFromPersister() {
	DPrintf("kvserver %d-%d installSnapFromPersister\n",kv.gid,kv.me)
	var snapshpot KVServerSnapshot= NewKVServerSnapshot()
	data := kv.persister.ReadSnapshot()
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	_ = decoder.Decode(&snapshpot)
	kv.us=snapshpot.Us
	if snapshpot.KVMap.Map==nil{
		kv.kvMap=NewShardMap()
		kv.lastIndex=0
		kv.config.Num=0
	}else{
		kv.lastIndex = snapshpot.LastIndex
		kv.kvMap = snapshpot.KVMap
		kv.config=snapshpot.Config.Copy()
		kv.shardNonces=copyShardNonce2( snapshpot.ShardNonce)
		kv.us=snapshpot.Us
		kv.configApplied=true
	}
	
	//kv.opIDSet = snapshpot.OpIDSet
}
