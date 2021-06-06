package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op = Op{
		Type:  GET,
		Key:   args.Key,
		Value: "",
		Nonce: args.Nonce,
	}
	kv.Lock()
	DPrintf("kvserver %d-%d received Get request %s with current state %s\n", kv.gid, kv.me, args, kv)
	shard := key2shard(args.Key)
	//check shard
	if kv.config.Num == 0 {
		DPrintf("kvserver %d-%d response wrondgroup Get request %s with %s\n", kv.gid, kv.me, args, reply)
		reply.Err = ErrWrongGroup
		kv.Unlock()
		return
	} else {

		if kv.config.Shards[shard] != kv.gid || /*!kv.kvMap.HasShard(shard)*/ !kv.configApplied || kv.config.Num != args.Num {
			reply.Err = ErrWrongGroup
			DPrintf("kvserver %d-%d response wrondgroup Get request %s with %s\n", kv.gid, kv.me, args, reply)
			kv.Unlock()
			return
		}
	}
	//check duplication
	if _, exist := kv.nonces[args.Nonce]; exist {
		//duplicate just fetch the result at once
		v, ok := kv.kvMap.Get(args.Key)
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = v
		}
		DPrintf("kvserver %d-%d response duplicated Get request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	if _, exist := kv.shardNonces[shard]; exist {
		if _, got := kv.shardNonces[shard][args.Nonce]; got {
			//duplicate just fetch the result at once
			v, ok := kv.kvMap.Get(args.Key)
			if !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = v
			}
			DPrintf("kvserver %d-%d response duplicated Get request %s with %s\n", kv.gid, kv.me, args, reply)
			kv.Unlock()
			return
		}
	}
	op.ConfigNum = kv.config.Num
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d-%d response  Get request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	//remember this index
	kv.indexToNonce[index] = args.Nonce
	//wait for response
	pendingChan := make(chan OpResult)
	if _, ok := kv.pendingChans[args.Nonce]; !ok {
		kv.pendingChans[args.Nonce] = make([]chan OpResult, 0)
	}
	kv.pendingChans[args.Nonce] = append(kv.pendingChans[args.Nonce], pendingChan)
	DPrintf("kvserver %d-%d wait Get request %s for reply, current state %s\n", kv.gid, kv.me, args, kv)
	kv.Unlock()

	opResult := <-pendingChan
	reply.Err = opResult.Err
	reply.Value = opResult.Value
	kv.Lock()
	DPrintf("kvserver %d-%d response Get request %s with %s\n", kv.gid, kv.me, args, reply)
	kv.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op = Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		Nonce: args.Nonce,
	}
	kv.Lock()
	DPrintf("kvserver %d-%d received Append request %s\n", kv.gid, kv.me, args)
	shard := key2shard(args.Key)
	//check shard
	if kv.config.Num == 0 {
		DPrintf("kvserver %d-%d response wrondgroup Get request %s with %s\n", kv.gid, kv.me, args, reply)
		reply.Err = ErrWrongGroup
		kv.Unlock()
		return
	} else {

		if kv.config.Shards[shard] != kv.gid || /*!kv.kvMap.HasShard(shard)*/ !kv.configApplied || kv.config.Num != args.Num {
			reply.Err = ErrWrongGroup
			DPrintf("kvserver %d-%d response wrondgroup Get request %s with %s\n", kv.gid, kv.me, args, reply)
			kv.Unlock()
			return
		}
	}
	//check duplication
	if _, exist := kv.nonces[args.Nonce]; exist {
		reply.Err = OK
		DPrintf("kvserver %d-%d response Get request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	if _, exist := kv.shardNonces[shard]; exist {
		if _, got := kv.shardNonces[shard][args.Nonce]; got {
			//duplicate just fetch the result at once
			reply.Err = OK
			DPrintf("kvserver %d-%d response Get request %s with %s\n", kv.gid, kv.me, args, reply)
			kv.Unlock()
			return
		}
	}
	op.ConfigNum = kv.config.Num
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d-%d response Append request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	//remember this index
	kv.indexToNonce[index] = args.Nonce
	//wait for response
	pendingChan := make(chan OpResult)
	if _, ok := kv.pendingChans[args.Nonce]; !ok {
		kv.pendingChans[args.Nonce] = make([]chan OpResult, 0)
	}
	kv.pendingChans[args.Nonce] = append(kv.pendingChans[args.Nonce], pendingChan)
	DPrintf("kvserver %d-%d wait Putappend request %s for reply, current state %s\n", kv.gid, kv.me, args, kv)
	kv.Unlock()
	opResult := <-pendingChan
	reply.Err = opResult.Err

	kv.Lock()
	DPrintf("kvserver %d-%d response  PutAppend request %s with %s\n", kv.gid, kv.me, args, reply)
	kv.Unlock()
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	var op = Op{
		Type:  GETSHARDS,
		Data:  *args,
		Nonce: args.Nonce,
	}
	kv.Lock()
	DPrintf("kvserver %d-%d received Getshard request %s, current state %s\n", kv.gid, kv.me, args, kv)
	//check shard
	if args.Num > kv.newConfig.Num {
		reply.Err = ErrWrongConfigNum
		DPrintf("kvserver %d-%d response Getshard request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	//check duplication
	if _, exist := kv.nonces[args.Nonce]; exist {
		reply.Err = OK
		DPrintf("kvserver %d-%d response  Getshard request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d-%d response  Getshard request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	//remember this index
	kv.indexToNonce[index] = args.Nonce
	//wait for response
	pendingChan := make(chan OpResult)
	if _, ok := kv.pendingChans[args.Nonce]; !ok {
		kv.pendingChans[args.Nonce] = make([]chan OpResult, 0)
	}
	kv.pendingChans[args.Nonce] = append(kv.pendingChans[args.Nonce], pendingChan)
	DPrintf("kvserver %d-%d wait Getshard request %s for reply, current state %s\n", kv.gid, kv.me, args, kv)
	kv.Unlock()

	opResult := <-pendingChan
	reply.Err = opResult.Err
	if reply.Err == OK {
		res := opResult.Data.(GetShardsReply)
		reply.Data = res.Data
		reply.ShardNonce=res.ShardNonce
	}

	kv.Lock()
	DPrintf("kvserver %d-%d response getshard request %s with %s\n", kv.gid, kv.me, args, reply)
	kv.Unlock()
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	var op = Op{
		Type:  INSTALLSHARDS,
		Nonce: args.Nonce,
		Data:  *args,
	}
	kv.Lock()
	DPrintf("kvserver %d-%d received install shard request %s, current state %s\n", kv.gid, kv.me, args, kv)
	//check shard
	if args.Num < kv.newConfig.Num {
		reply.Err = OK
		DPrintf("kvserver %d-%d response InstallShard request %s with %s,current state %s\n", kv.gid, kv.me, args, reply, kv)
		kv.Unlock()
		return
	}
	//check duplication
	if _, exist := kv.nonces[args.Nonce]; exist {
		reply.Err = OK
		DPrintf("kvserver %d-%d response InstallShard request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d-%d response InstallShard request %s with %s\n", kv.gid, kv.me, args, reply)
		kv.Unlock()
		return
	}
	//remember this index
	kv.indexToNonce[index] = args.Nonce
	//wait for response
	pendingChan := make(chan OpResult)
	if _, ok := kv.pendingChans[args.Nonce]; !ok {
		kv.pendingChans[args.Nonce] = make([]chan OpResult, 0)
	}
	kv.pendingChans[args.Nonce] = append(kv.pendingChans[args.Nonce], pendingChan)
	kv.Unlock()

	opResult := <-pendingChan
	reply.Err = opResult.Err

	kv.Lock()
	DPrintf("kvserver %d-%d response InstallShard request %s with %s\n", kv.gid, kv.me, args, reply)
	kv.Unlock()

}
