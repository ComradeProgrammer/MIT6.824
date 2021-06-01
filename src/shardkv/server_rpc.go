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
	DPrintf("kvserver %d received Get request %s\n", kv.me, args)
	//check shard
	if kv.config.Num == 0 {
		DPrintf("kvserver %d response wrondgroup Get request %s with %s\n", kv.me, args, reply)
		reply.Err = ErrWrongGroup
		kv.Unlock()
		return
	} else {
		shard := key2shard(args.Key)
		if kv.config.Shards[shard] != kv.gid {
			DPrintf("kvserver %d response wrondgroup Get request %s with %s\n", kv.me, args, reply)
			reply.Err = ErrWrongGroup
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
		DPrintf("kvserver %d response duplicated Get request %s with %s\n", kv.me, args, reply)
		kv.Unlock()
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d response  Get request %s with %s\n", kv.me, args, reply)
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
	reply.Value = opResult.Value
	kv.Lock()
	DPrintf("kvserver %d response  Get request %s with %s\n", kv.me, args, reply)
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
	DPrintf("kvserver %d received Append request %s\n", kv.me, args)
	//check shard
	if kv.config.Num == 0 {
		DPrintf("kvserver %d response wrondgroup Get request %s with %s\n", kv.me, args, reply)
		reply.Err = ErrWrongGroup
		kv.Unlock()
		return
	} else {
		shard := key2shard(args.Key)
		if kv.config.Shards[shard] != kv.gid {
			DPrintf("kvserver %d response wrondgroup Get request %s with %s\n", kv.me, args, reply)
			reply.Err = ErrWrongGroup
			kv.Unlock()
			return
		}
	}
	//check duplication
	if _, exist := kv.nonces[args.Nonce]; exist {
		reply.Err = OK
		DPrintf("kvserver %d response duplicated Get request %s with %s\n", kv.me, args, reply)
		kv.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kvserver %d response duplicated Append request %s with %s\n", kv.me, args, reply)
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
	DPrintf("kvserver %d response  Get request %s with %s\n", kv.me, args, reply)
	kv.Unlock()
}
