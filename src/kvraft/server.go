package kvraft

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	ID int64
}

type OpResult struct{
	Err Err
	Value string
}

type KVServer struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string
	commitChanMap map[int64]chan OpResult
	abortChanMap map[int64]chan struct{}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	
	var op=Op{
		Type:GET,
		Key:args.Key,
		Value: "",
		ID: args.ID,
	} 
	kv.Lock()
	_,_,isLeader:=kv.rf.Start(op)
	if !isLeader{
		reply.Err=ErrWrongLeader
		kv.Unlock()
		return
	}
	DPrintf("kvserver %d received Get request %s\n",kv.me,args)
	abortChan:=make(chan struct{})
	commitChan:=make(chan OpResult)
	kv.abortChanMap[op.ID]=abortChan
	kv.commitChanMap[op.ID]=commitChan
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
	_,_,isLeader:=kv.rf.Start(op)
	if !isLeader{
		reply.Err=ErrWrongLeader
		kv.Unlock()
		return
	}
	DPrintf("kvserver %d received Append request %s\n",kv.me,args)
	abortChan:=make(chan struct{})
	commitChan:=make(chan OpResult)
	kv.abortChanMap[op.ID]=abortChan
	kv.commitChanMap[op.ID]=commitChan
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer)applyChThread(){
	for !kv.killed(){
		applyMsg:=<-kv.applyCh
		kv.Lock()
		DPrintf("kvserver %d got applyMessage %v\n",kv.me,applyMsg)
		//handle the applyMsg
		if applyMsg.CommandValid{
			command:=applyMsg.Command.(Op)
			if command.Type==GET{
				//get won't modify the server state,so just seek for pending request
				if ch,ok:=kv.commitChanMap[command.ID];ok{
					//there is a pending request
					opResult:=OpResult{}
					value,exist:=kv.kvMap[command.Key]
					if exist{
						opResult.Err=OK
						opResult.Value=value
					}else{
						opResult.Err=ErrNoKey
					}
					delete(kv.commitChanMap,command.ID)
					delete(kv.abortChanMap,command.ID)
					kv.Unlock()
					DPrintf("kvserver %d reply applyMessage %v with %v\n",kv.me,applyMsg,opResult)
					ch<-opResult
				}else{
					DPrintf("kvserver %d discard applyMessage %v\n",kv.me,applyMsg)
					kv.Unlock()
				} 
			}else if command.Type==APPEND{
				if ch,ok:=kv.commitChanMap[command.ID];ok{
					opResult:=OpResult{}
					value,exist:=kv.kvMap[command.Key]
					if exist{
						kv.kvMap[command.Key]=value+command.Value
					}else{
						kv.kvMap[command.Key]=command.Value
					}
					opResult.Err=OK
					delete(kv.commitChanMap,command.ID)
					delete(kv.abortChanMap,command.ID)
					kv.Unlock()
					DPrintf("kvserver %d reply applyMessage %v with %v\n",kv.me,applyMsg,opResult)
					ch<-opResult
				}else{
					value,exist:=kv.kvMap[command.Key]
					if exist{
						kv.kvMap[command.Key]=value+command.Value
					}else{
						kv.kvMap[command.Key]=command.Value
					}
					DPrintf("kvserver %d apply applyMessage %v\n",kv.me,applyMsg)
					kv.Unlock()
				}	
			}else if command.Type==PUT{
				if ch,ok:=kv.commitChanMap[command.ID];ok{
					opResult:=OpResult{}
					kv.kvMap[command.Key]=command.Value
					opResult.Err=OK
					delete(kv.commitChanMap,command.ID)
					delete(kv.abortChanMap,command.ID)
					kv.Unlock()
					DPrintf("kvserver %d reply applyMessage %v with %v\n",kv.me,applyMsg,opResult)
					ch<-opResult
				}else{
					DPrintf("kvserver %d apply applyMessage %v\n",kv.me,applyMsg)
					kv.Unlock()
				}	
			}
		}
		
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.abortChanMap=make(map[int64]chan struct{})
	kv.commitChanMap=make(map[int64]chan OpResult)
	kv.kvMap=make(map[string]string)
	go kv.applyChThread()
	return kv
}
