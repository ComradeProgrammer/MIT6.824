package shardkv

import (
	"time"

	"6.824/raft"
)

func (kv *ShardKV) applyTicker() {
	for {
		kv.checkLeaderShip()
		var applyMsg raft.ApplyMsg
		select {
		case <-kv.done:
			return
		case applyMsg = <-kv.applyCh:
		default:
			time.Sleep(20 * time.Millisecond)
			continue
		}

		kv.Lock()
		DPrintf("kvserver %d-%d receive ApplyMsg %v with current state %s \n",kv.me, kv.me, applyMsg,kv)
		kv.checkMaxSizeExceeded()
		if applyMsg.CommandValid {
			ok := kv.checkRequest(&applyMsg)
			if !ok {
				kv.Unlock()
				continue
			}
			op := applyMsg.Command.(Op)
			var opResult OpResult
			switch op.Type {
			case GET:
				opResult = kv.handleGet(applyMsg)
			case APPEND:
				opResult = kv.handleAppend(applyMsg)
			case PUT:
				opResult = kv.handlePut(applyMsg)
			}
			//send out request
			chs, ok := kv.pendingChans[op.Nonce]
			delete(kv.pendingChans, op.Nonce)
			kv.Unlock()
			if ok {
				for _, ch := range chs {
					ch <- opResult
				}
				kv.Lock()
				DPrintf("kvserver %d-%d reply applymsg %v with %v\n",kv.gid, kv.me, applyMsg, opResult)
				kv.Unlock()
			}
		}else{
			kv.handleSnapInstall(&applyMsg)
			kv.Unlock()
		}

	}

}
func (kv *ShardKV) checkLeaderShip() {
	term, isLeader := kv.rf.GetState()
	if kv.isLeader&& !isLeader || kv.term != term {
		kv.term = term
		//kick out all hanging request
		kv.Lock()
		defer kv.Unlock()
		DPrintf("all pending request is kicked out due to leadership change\n")
		delList := make([]int64, 0)
		for k, chs := range kv.pendingChans {
			for _, ch := range chs {
				ch <- OpResult{Err: ErrWrongLeader}
			}
			delList = append(delList, k)
		}
		for _, k := range delList {
			delete(kv.pendingChans, k)
		}
	}
	kv.isLeader=isLeader
}

//return whether to continue to handle next applyMsg
func (kv *ShardKV) checkRequest(applyMsg *raft.ApplyMsg) bool {
	//1 .check whether the index conflicts with the old index
	nonce, ok := kv.indexToNonce[applyMsg.CommandIndex]
	op := applyMsg.Command.(Op)
	if ok {

		if op.Nonce != nonce {
			//raft state change! kick all pending interests
			DPrintf("all pending request is kicked out due to incorrect nonce\n")
			delList := make([]int64, 0)
			for k, chs := range kv.pendingChans {
				for _, ch := range chs {
					ch <- OpResult{Err: ErrWrongLeader}
				}
				delList = append(delList, k)
			}
			for _, k := range delList {
				delete(kv.pendingChans, k)
			}
		}
	}
	//2. check whether this is a duplicate applyMsg
	if _, ok := kv.nonces[op.Nonce]; ok {
		DPrintf("shardctrl %d-%d drop duplicate ApplyMsg %v \n",kv.gid, kv.me, applyMsg)
		return false
	}
	//3. add this nonce to nonce set
	kv.nonces[op.Nonce] = struct{}{}
	return true
}

func (kv *ShardKV) handleGet(applyMsg raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	value, ok := kv.kvMap.Get(op.Key)
	var res = OpResult{}
	if !ok {
		res.Err = ErrNoKey
	} else {
		res.Err = OK
		res.Value = value
	}
	return res
}

func (kv *ShardKV) handlePut(applyMsg raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	kv.kvMap.Put(op.Key, op.Value)
	var res = OpResult{}
	res.Err = OK
	return res
}

func (kv *ShardKV) handleAppend(applyMsg raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	kv.kvMap.Append(op.Key, op.Value)
	var res = OpResult{}
	res.Err = OK
	return res
}
