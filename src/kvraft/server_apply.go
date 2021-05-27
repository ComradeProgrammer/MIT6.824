package kvraft

import (
	"time"

	"6.824/raft"
)

func (kv *KVServer) abortOp(id int64) {
	chs := kv.abortChanMap[id]
	delete(kv.commitChanMap, id)
	delete(kv.abortChanMap, id)
	for _, ch := range chs {
		ch <- struct{}{}
	}
}

func (kv *KVServer) commitGetOp(command Op) OpResult {
	opResult := OpResult{}
	value, exist := kv.kvMap[command.Key]
	if exist {
		opResult.Err = OK
		opResult.Value = value
	} else {
		opResult.Err = ErrNoKey
	}
	//record this ID
	kv.opIDSet[command.ID] = struct{}{}
	return opResult

}

func (kv *KVServer) commitAppendOp(command Op) OpResult {
	opResult := OpResult{}
	//whether this is a duplicated request
	if _, ok := kv.opIDSet[command.ID]; !ok {
		//no
		value, exist := kv.kvMap[command.Key]
		if exist {
			kv.kvMap[command.Key] = value + command.Value
		} else {
			kv.kvMap[command.Key] = command.Value
		}
		DPrintf("kvserver %d modify key %s to %s when handling command %v",kv.me,command.Key,kv.kvMap[command.Key],command)
		kv.opIDSet[command.ID]=struct{}{}
	}else{
		//yes
		DPrintf("kvserver %d drop duplicate append request %v\n",kv.me,command)
	}
	opResult.Err = OK
	return opResult
}

func (kv *KVServer) commitPutOp(command Op) OpResult {
	opResult := OpResult{}
	//whether this is a duplicated request
	if _, ok := kv.opIDSet[command.ID]; !ok {
		//no
		kv.kvMap[command.Key] = command.Value
		DPrintf("kvserver %d modify key %s to %s when handling command %v",kv.me,command.Key,kv.kvMap[command.Key],command)
		kv.opIDSet[command.ID]=struct{}{}
	}else{
		//yes
		DPrintf("kvserver %d drop duplicate append request %v\n",kv.me,command)
	}
	opResult.Err = OK
	return opResult
}

func (kv *KVServer) applyChThread() {
	var oldterm=0
	for !kv.killed() {
		term,isLeader:=kv.rf.GetState()
		if !isLeader||term!=oldterm{
			oldterm=term
			//state change! abort all pending request
			kv.Lock()
			for k, _ := range kv.abortChanMap {
				kv.abortOp(k)
			}
			kv.Unlock()
		}
		var applyMsg raft.ApplyMsg
		select{
		case applyMsg = <-kv.applyCh:
		default:
			time.Sleep(20*time.Millisecond)
			continue
		}

		
		DPrintf("kvserver got applyMessage %v\n", applyMsg)
		kv.Lock()
		DPrintf("kvserver %d got applyMessage %v\n", kv.me, applyMsg)
		//handle the applyMsg
		if applyMsg.CommandValid {
			command := applyMsg.Command.(Op)
			//detect abnormal case that corresponding raft server has lost its leadership
			//by checking the index
			index := applyMsg.CommandIndex
			if id, ok := kv.raftIndexToOp[index]; ok {
				if id != command.ID {
					//this op should be aborted at once
					kv.abortOp(id)
					//by the way, if this raft is currently not leader,then all ops shoulde be abandoned at once
					if _, isLeader := kv.rf.GetState(); !isLeader {
						for k, _ := range kv.abortChanMap {
							kv.abortOp(k)
						}
					}
				}
			}

			if command.Type == GET {
				//get won't modify the server state,so just seek for pending request
				if chs, ok := kv.commitChanMap[command.ID]; ok {
					//there is a pending request
					opResult := kv.commitGetOp(command)
					delete(kv.commitChanMap, command.ID)
					delete(kv.abortChanMap, command.ID)
					DPrintf("kvserver %d reply applyMessage %v with %v\n", kv.me, applyMsg, opResult)
					kv.Unlock()
					for _, ch := range chs {
						ch <- opResult
					}
				} else {
					DPrintf("kvserver %d accept applyMessage %v without delivery\n", kv.me, applyMsg)
					kv.Unlock()
				}
			} else if command.Type == APPEND {
				if chs, ok := kv.commitChanMap[command.ID]; ok {
					//当前server有挂起的对应任务，除了修改键值对还要响应任务
					opResult:=kv.commitAppendOp(command)
					delete(kv.commitChanMap, command.ID)
					delete(kv.abortChanMap, command.ID)
					DPrintf("kvserver %d reply applyMessage %v with %v\n", kv.me, applyMsg, opResult)
					kv.Unlock()
					for _, ch := range chs {
						ch <- opResult
					}
				} else {
					//否则只修改键值对
					kv.commitAppendOp(command)
					DPrintf("kvserver %d accept applyMessage %v without delivery\n", kv.me, applyMsg)
					kv.Unlock()
				}
			} else if command.Type == PUT {
				if chs, ok := kv.commitChanMap[command.ID]; ok {
					//当前server有挂起的对应任务，除了修改键值对还要响应任务
					opResult := kv.commitPutOp(command)
					delete(kv.commitChanMap, command.ID)
					delete(kv.abortChanMap, command.ID)
					DPrintf("kvserver %d reply applyMessage %v with %v\n", kv.me, applyMsg, opResult)
					kv.Unlock()
					for _, ch := range chs {
						ch <- opResult
					}
				} else {
					//否则只修改键值对
					kv.commitPutOp(command)
					DPrintf("kvserver %d accept applyMessage %v without delivery\n", kv.me, applyMsg)
					kv.Unlock()
				}
			}
		}

	}
	//DPrintf("return here")
}
