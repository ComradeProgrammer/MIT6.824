package kvraft

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
)

type KVServerSnapshot struct {
	KVMap     map[string]string
	LastIndex int
	//OpIDSet   map[int64]struct{}
}
func NewKVServerSnapshot()KVServerSnapshot{
	return  KVServerSnapshot{
		KVMap: make(map[string]string),
		//OpIDSet: make(map[int64]struct{}),
	}
}

func (kv *KVServer) checkMaxSizeExceeded() {
	if kv.maxraftstate==-1{
		return
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate/5*4 && kv.lastIndex != 0 {
		DPrintf("kvserver %d start snapshop with lastIndex %d\n", kv.me, kv.lastIndex)
		//80% as the threshold, start snapshot
		snapShot := KVServerSnapshot{
			KVMap:     kv.kvMap,
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

func (kv *KVServer) handleSnapInstall(msg *raft.ApplyMsg) {
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

func (kv *KVServer) installSnapFromPersister() {
	var snapshpot KVServerSnapshot= NewKVServerSnapshot()
	data := kv.persister.ReadSnapshot()
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	_ = decoder.Decode(&snapshpot)
	kv.lastIndex = snapshpot.LastIndex
	kv.kvMap = snapshpot.KVMap
	//kv.opIDSet = snapshpot.OpIDSet
}
