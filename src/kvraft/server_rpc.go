package kvraft
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	
	var op=Op{
		Type:GET,
		Key:args.Key,
		Value: "",
		ID: args.ID,
	} 
	kv.Lock()
	//check duplicated:
	if _,ok:=kv.opIDSet[op.ID];ok{
		//this id a duplicated request
		//directly give out result
		if v,exist:=kv.kvMap[op.Key];exist{
			reply.Err=OK
			reply.Value=v
		}else{
			reply.Err=ErrNoKey
		}
		DPrintf("kvserver %d response duplicated Get request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	DPrintf("kvserver %d received Get request %s\n",kv.me,args)
	index,_,isLeader:=kv.rf.Start(op)
	if !isLeader{
		reply.Err=ErrWrongLeader
		DPrintf("kvserver %d response duplicated Get request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	
	
	kv.raftIndexToOp[index]=op.ID
	abortChan:=make(chan struct{})
	commitChan:=make(chan OpResult)
	
	kv.pendRequest(op.ID,abortChan,commitChan)
	kv.Unlock()
	//then we wait for signal
	select{
	case res:=<-commitChan:
		reply.Err=res.Err
		reply.Value=res.Value
		kv.Lock()
		DPrintf("kvserver %d response Get request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	case <-abortChan:
		reply.Err=ErrWrongLeader
		kv.Lock()
		DPrintf("kvserver %d response Get request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op=Op{
		Type: args.Op,
		Key:args.Key,
		Value: args.Value,
		ID:args.ID,
	}
	kv.Lock()
	//check duplicate
	if _,ok:=kv.opIDSet[op.ID];ok{
		reply.Err=OK
		DPrintf("kvserver %d response duplicated Append request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	DPrintf("kvserver %d received Append request %s\n",kv.me,args)
	index,_,isLeader:=kv.rf.Start(op)
	if !isLeader{
		reply.Err=ErrWrongLeader
		DPrintf("kvserver %d response duplicated Append request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	kv.raftIndexToOp[index]=op.ID
	
	abortChan:=make(chan struct{})
	commitChan:=make(chan OpResult)
	kv.pendRequest(op.ID,abortChan,commitChan)
	kv.Unlock()

	select{
	case res:=<-commitChan:
		reply.Err=res.Err
		kv.Lock()
		DPrintf("kvserver %d response Append request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	case <-abortChan:
		reply.Err=ErrWrongLeader
		kv.Lock()
		DPrintf("kvserver %d response Append request %s with %s\n",kv.me,args,reply)
		kv.Unlock()
		return
	}
	
}

func(kv *KVServer)pendRequest(opID int64,abort chan struct{} , commit chan OpResult)(){
	if _,ok:=kv.abortChanMap[opID];!ok{
		kv.abortChanMap[opID]=make([]chan struct{}, 0)
		kv.commitChanMap[opID]=make([]chan OpResult, 0)
	}
	kv.abortChanMap[opID]=append(kv.abortChanMap[opID],abort)
	kv.commitChanMap[opID]=append(kv.commitChanMap[opID], commit)
}
