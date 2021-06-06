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
			kv.Lock()
			DPrintf("kvserver %d-%d applyticker quited\n",kv.gid, kv.me)
			kv.Unlock()
			return
		case applyMsg = <-kv.applyCh:
		default:
			time.Sleep(20 * time.Millisecond)
			continue
		}

		kv.Lock()
		DPrintf("kvserver %d-%d receive ApplyMsg %v with current state %s \n",kv.gid, kv.me, applyMsg,kv)
		kv.checkMaxSizeExceeded()
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			if index>kv.lastIndex{
				kv.lastIndex=index
			}
			ok := kv.checkRequest(&applyMsg)
			if !ok {
				kv.Unlock()
				continue
			}
			op := applyMsg.Command.(Op)
			var opResult OpResult
			switch op.Type {
			case GET:
				opResult = kv.handleGet(&applyMsg)
			case APPEND:
				opResult = kv.handleAppend(&applyMsg)
			case PUT:
				opResult = kv.handlePut(&applyMsg)
			case GETSHARDS:
				opResult = kv.handleGetShards(&applyMsg)
			case INSTALLSHARDS:
				opResult=kv.handleInstallShards(&applyMsg)
			}
			DPrintf("kvserver %d-%d apply applymsg %v with %v,\n\tcurrent state %v\n",kv.gid, kv.me, applyMsg, opResult,kv)
			//send out request
			chs, ok := kv.pendingChans[op.Nonce]
			delete(kv.pendingChans, op.Nonce)
			kv.Unlock()
			if ok {
				for _, ch := range chs {
					ch <- opResult
				}
				kv.Lock()
				DPrintf("kvserver %d-%d reply applymsg %v with %v,\n\tcurrent state %v\n",kv.gid, kv.me, applyMsg, opResult,kv)
				kv.Unlock()
			}
		}else if applyMsg.SnapshotValid{
			kv.handleSnapInstall(&applyMsg)
			kv.Unlock()
		}

	}

}
func (kv *ShardKV) checkLeaderShip() {
	term, isLeader := kv.rf.GetState()
	kv.Lock()
	defer kv.Unlock()
	if kv.isLeader&& !isLeader || kv.term != term {
		kv.term = term
		//kick out all hanging request
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

func (kv *ShardKV) handleGet(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	var res = OpResult{}
	if op.ConfigNum<kv.config.Num{
		res.Err=ErrWrongConfigNum
		delete(kv.nonces,op.Nonce)
		return res
	}
	shard:=key2shard(op.Key)


	if _,ok:=kv.shardNonces[shard];!ok{
		kv.shardNonces[shard]=map[int64]struct{}{}
	}
	kv.shardNonces[shard][op.Nonce]=struct{}{}
	value, ok := kv.kvMap.Get(op.Key)
	if !ok {
		res.Err = ErrNoKey
	} else {
		res.Err = OK
		res.Value = value
	}
	return res
}

func (kv *ShardKV) handlePut(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	var res = OpResult{}
	if op.ConfigNum<kv.config.Num{
		res.Err=ErrWrongConfigNum
		delete(kv.nonces,op.Nonce)
		return res
	}
	shard:=key2shard(op.Key)
	if _, exist := kv.shardNonces[shard]; exist {
		if _, got := kv.shardNonces[shard][op.Nonce]; got {
			//duplicate just fetch the result at once
			res.Err = OK
			return res
		}
	}
	if _,ok:=kv.shardNonces[shard];!ok{
		kv.shardNonces[shard]=map[int64]struct{}{}
	}
	kv.shardNonces[shard][op.Nonce]=struct{}{}
	kv.kvMap.Put(op.Key, op.Value)
	
	res.Err = OK
	return res
}

func (kv *ShardKV) handleAppend(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	var res = OpResult{}
	if op.ConfigNum<kv.config.Num{
		res.Err=ErrWrongConfigNum
		delete(kv.nonces,op.Nonce)
		return res
	}
	shard:=key2shard(op.Key)
	if _, exist := kv.shardNonces[shard]; exist {
		if _, got := kv.shardNonces[shard][op.Nonce]; got {
			//duplicate just fetch the result at once
			res.Err = OK
			return res
		}
	}
	if _,ok:=kv.shardNonces[shard];!ok{
		kv.shardNonces[shard]=map[int64]struct{}{}
	}
	kv.shardNonces[shard][op.Nonce]=struct{}{}
	kv.kvMap.Append(op.Key, op.Value)
	res.Err = OK
	return res
}

func(kv *ShardKV)handleGetShards(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg:=op.Data.(GetShardsArgs)
	reply:=GetShardsReply{
		Data: make(map[int]map[string]string),
		Err: OK,
		ShardNonce: map[int]map[int64]struct{}{},
	}
	cp:=kv.copyShardNonce()
	for _,shard:=range arg.Shards{
		if kv.kvMap.HasShard(shard){
			reply.Data[shard]=kv.kvMap.ExportShard(shard)
		}
		reply.ShardNonce[shard]=cp[shard]
	}
	var res =OpResult{
		Err: reply.Err,
		Data: reply,
	}
	return res
}
func(kv *ShardKV)handleInstallShards(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg:=op.Data.(InstallShardArgs)
	for s,m:=range arg.Data{
		kv.kvMap.ImportShard(s,m)
	}
	for s,m:=range copyShardNonce2( arg.ShardNonce){
		kv.shardNonces[s]=m
	}
	kv.config=arg.Config.Copy()
	kv.configApplied=true
	if len(kv.config.Groups[kv.gid])!=0{

		kv.us=kv.config.Groups[kv.gid]
	}
	var res =OpResult{
		Err: OK,
	}
	return res
}