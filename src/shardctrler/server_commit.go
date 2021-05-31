package shardctrler

import (
	"sort"
	"time"

	"6.824/raft"
)

type Op struct {
	// Your data here.
	Type  string
	Nonce int64
	Args  interface{}
}

type OpResult struct {
	Success bool
	Err     Err
	Config  Config
}

type IntSlice []int

func (s IntSlice) Len() int           { return len(s) }
func (s IntSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s IntSlice) Less(i, j int) bool { return s[i] < s[j] }

func (sc *ShardCtrler) applyTicker() {
	for {
		var applyMsg raft.ApplyMsg
		sc.checkLeaderShip()
		select {
		case <-sc.terminate:
			//we are done
			return
		case applyMsg = <-sc.applyCh:
			//nothing, go to handle this applyMsg
		default:
			//go to next loop
			time.Sleep(10 * time.Millisecond)
			continue
		}
		//handle this applyMsg

		sc.Lock()
		DPrintf("shardctrl %d receive ApplyMsg %v \n", sc.me, applyMsg)
		ok := sc.checkRequest(&applyMsg)
		if !ok {
			sc.Unlock()
			continue
		}

		op := applyMsg.Command.(Op)
		var res OpResult
		switch op.Type {
		case JOIN:
			res=sc.handleJoin(&applyMsg)
		case LEAVE:
			res=sc.handleLeave(&applyMsg)
		case MOVE:
			res=sc.handleMove(&applyMsg)
		case QUERY:
			res=sc.handleQuery(&applyMsg)
		}
		chs,ok:=sc.hangingChannels[op.Nonce];
		delete(sc.hangingChannels,op.Nonce)
		sc.Unlock()
		if ok{
			for _,ch:=range chs{
				ch<-res
			}
			sc.Lock()
			DPrintf("server %d reply applymsg %v with %v\n",sc.me,applyMsg,res)
			sc.Unlock()
		}

	}
}

func (sc *ShardCtrler) checkLeaderShip() {
	term, isLeader := sc.rf.GetState()
	if !isLeader || sc.term != term {
		sc.term = term
		//kick out all hanging request
		sc.Lock()
		defer sc.Unlock()

		delList := make([]int64, 0)
		for k, chs := range sc.hangingChannels {
			for _, ch := range chs {
				ch <- OpResult{Err: WRONGLEADER, Success: false}
			}
			delList = append(delList, k)
		}
		for _, k := range delList {
			delete(sc.hangingChannels, k)
		}
	}
}

//return whether to continue to handle next applyMsg
func (sc *ShardCtrler) checkRequest(applyMsg *raft.ApplyMsg) bool {
	//1 .check whether the index conflicts with the old index
	nonce, ok := sc.indexToNonce[applyMsg.CommandIndex]
	op := applyMsg.Command.(Op)
	if ok {

		if op.Nonce != nonce {
			//raft state change! kick all pending interests
			DPrintf("all pending request is kicked out due to incorrect nonce\n")
			delList := make([]int64, 0)
			for k, chs := range sc.hangingChannels {
				for _, ch := range chs {
					ch <- OpResult{Err: WRONGLEADER, Success: false}
				}
				delList = append(delList, k)
			}
			for _, k := range delList {
				delete(sc.hangingChannels, k)
			}
		}
	}
	//2. check whether this is a duplicate applyMsg
	if _, ok := sc.nonces[op.Nonce]; ok {
		DPrintf("shardctrl %d drop duplicate ApplyMsg %v \n", sc.me, applyMsg)
		return false
	}
	//3. add this nonce to nonce set
	sc.nonces[op.Nonce] = struct{}{}
	return true
}

func (sc *ShardCtrler) handleJoin(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg := op.Args.(JoinArgs)
	for k, v := range arg.Servers {
		sc.gids = append(sc.gids, k)
		sc.gidToServers[k] = v
	}
	newConfig := sc.createConfig()
	//store it
	sc.configs = append(sc.configs, newConfig)

	DPrintf("server %d accept join with current divide %v\n", sc.me, newConfig.Shards)
	res := OpResult{
		Success: true,
		Err:     OK,
	}
	return res
}
func (sc *ShardCtrler) handleLeave(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg := op.Args.(LeaveArgs)
	for _, gid := range arg.GIDs {
		delete(sc.gidToServers, gid)
		for i := 0; i < len(sc.gids); i++ {
			if sc.gids[i] == gid {
				//kick it out
				sc.gids = append(sc.gids[0:i], sc.gids[i+1:]...)
			}
		}
	}
	newConfig := sc.createConfig()
	//store it
	sc.configs = append(sc.configs, newConfig)
	DPrintf("server %d accept join with current divide %v\n", sc.me, newConfig.Shards)
	res := OpResult{
		Success: true,
		Err:     OK,
	}
	return res

}
func (sc *ShardCtrler) handleQuery(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg := op.Args.(QueryArgs)
	index := arg.Num
	res := OpResult{
		Success: true,
		Err:     OK,
	}
	if index >= 0 && index < len(sc.configs) {
		res.Config = sc.configs[index].Copy()
	} else {
		res.Config = sc.configs[len(sc.configs)-1].Copy()
	}
	return res

}
func (sc *ShardCtrler) handleMove(applyMsg *raft.ApplyMsg) OpResult {
	op := applyMsg.Command.(Op)
	arg := op.Args.(MoveArgs)
	newConfig:= sc.configs[len(sc.configs)-1].Copy()
	newConfig.Shards[arg.Shard]=arg.GID
	newConfig.Num=len(sc.configs)
	sc.configs = append(sc.configs, newConfig)
	res := OpResult{
		Success: true,
		Err:     OK,
	}
	return res
}

func (sc *ShardCtrler) createConfig() Config {
	sort.Sort(IntSlice(sc.gids))
	//create a new config
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	// copy the Groups
	for k, v := range sc.gidToServers {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}
	//assign the shard
	if len(sc.gids)!=0{
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = sc.gids[i%len(sc.gids)]
		}
	}
	
	return newConfig
}
