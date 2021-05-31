package shardctrler
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.Lock()
	DPrintf("shardctrl %d received Join request with args %s\n",sc.me,args)
	//duplication detection
	if _,ok:=sc.nonces[args.Nonce];ok{
		//duplicate message
		reply.WrongLeader=false
		reply.Err=OK
		DPrintf("shardctrl %d answer Join request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	var op=Op{
		Type: JOIN,
		Nonce: args.Nonce,
		Args: *args,
	}
	index,_,isLeader:=sc.rf.Start(op)
	if !isLeader{
		//if server doesn't obtain leadership
		reply.WrongLeader=true
		DPrintf("shardctrl %d answer Join request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	//remember this index
	sc.indexToNonce[index]=args.Nonce
	//wait for result from raft
	hangingChan:=make(chan OpResult)
	if _,ok:=sc.hangingChannels[args.Nonce];!ok{
		sc.hangingChannels[args.Nonce]=make([]chan OpResult, 0)
	}
	sc.hangingChannels[args.Nonce]=append(sc.hangingChannels[args.Nonce], hangingChan)
	sc.Unlock()

	opResult:=<-hangingChan
	reply.WrongLeader=false
	if opResult.Success{
		reply.Err=OK
	}else{
		reply.Err=opResult.Err
	}
	sc.Lock()
	DPrintf("shardctrl %d answer Join request %s with %s\n",sc.me,args,reply)
	sc.Unlock()

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.Lock()
	DPrintf("shardctrl %d received Leave request with args %s\n",sc.me,args)
	//duplication detection
	if _,ok:=sc.nonces[args.Nonce];ok{
		//duplicate message
		reply.WrongLeader=false
		reply.Err=OK
		DPrintf("shardctrl %d answer Leave request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	var op=Op{
		Type: LEAVE,
		Nonce: args.Nonce,
		Args: *args,
	}
	_,_,isLeader:=sc.rf.Start(op)
	if !isLeader{
		//if server doesn't obtain leadership
		reply.WrongLeader=true
		DPrintf("shardctrl %d answer Leave request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	//wait for result from raft
	hangingChan:=make(chan OpResult)
	if _,ok:=sc.hangingChannels[args.Nonce];!ok{
		sc.hangingChannels[args.Nonce]=make([]chan OpResult, 0)
	}
	sc.hangingChannels[args.Nonce]=append(sc.hangingChannels[args.Nonce], hangingChan)
	sc.Unlock()

	opResult:=<-hangingChan
	reply.WrongLeader=false
	if opResult.Success{
		reply.Err=OK
	}else{
		reply.Err=opResult.Err
	}
	sc.Lock()
	DPrintf("shardctrl %d answer Leave request %s with %s\n",sc.me,args,reply)
	sc.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.Lock()
	DPrintf("shardctrl %d received Move request with args %s\n",sc.me,args)
	//duplication detection
	if _,ok:=sc.nonces[args.Nonce];ok{
		//duplicate message
		reply.WrongLeader=false
		reply.Err=OK
		DPrintf("shardctrl %d answer Move request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	var op=Op{
		Type: MOVE,
		Nonce: args.Nonce,
		Args: *args,
	}
	_,_,isLeader:=sc.rf.Start(op)
	if !isLeader{
		//if server doesn't obtain leadership
		reply.WrongLeader=true
		DPrintf("shardctrl %d answer Move request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	//wait for result from raft
	hangingChan:=make(chan OpResult)
	if _,ok:=sc.hangingChannels[args.Nonce];!ok{
		sc.hangingChannels[args.Nonce]=make([]chan OpResult, 0)
	}
	sc.hangingChannels[args.Nonce]=append(sc.hangingChannels[args.Nonce], hangingChan)
	sc.Unlock()

	opResult:=<-hangingChan
	reply.WrongLeader=false
	if opResult.Success{
		reply.Err=OK
	}else{
		reply.Err=opResult.Err
	}
	sc.Lock()
	DPrintf("shardctrl %d answer Move request %s with %s\n",sc.me,args,reply)
	sc.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Your code here.
	sc.Lock()
	DPrintf("shardctrl %d received Query request with args %s\n",sc.me,args)
	//duplication detection
	if _,ok:=sc.nonces[args.Nonce];ok{
		//duplicate message
		reply.WrongLeader=false
		if args.Num<0||args.Num>=len(sc.configs){
			reply.Config= sc.configs[len(sc.configs)-1];
		}else{
			reply.Config=sc.configs[args.Num]
		}
		reply.Err=OK
		DPrintf("shardctrl %d answer Query request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	var op=Op{
		Type: QUERY,
		Nonce: args.Nonce,
		Args: *args,
	}
	_,_,isLeader:=sc.rf.Start(op)
	if !isLeader{
		//if server doesn't obtain leadership
		reply.WrongLeader=true
		DPrintf("shardctrl %d answer Query request %s with %s\n",sc.me,args,reply)
		sc.Unlock()
		return
	}
	//wait for result from raft
	hangingChan:=make(chan OpResult)
	if _,ok:=sc.hangingChannels[args.Nonce];!ok{
		sc.hangingChannels[args.Nonce]=make([]chan OpResult, 0)
	}
	sc.hangingChannels[args.Nonce]=append(sc.hangingChannels[args.Nonce], hangingChan)
	sc.Unlock()
	opResult:=<-hangingChan
	reply.WrongLeader=false
	if opResult.Success{
		reply.Err=OK
	}else{
		reply.Err=opResult.Err
	}
	reply.Config=opResult.Config
	sc.Lock()
	DPrintf("shardctrl %d answer Query request %s with %s\n",sc.me,args,reply)
	sc.Unlock()
}
