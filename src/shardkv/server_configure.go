package shardkv

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"6.824/shardctrler"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (kv *ShardKV)pullConfiguration(){
	for{
		select{
		case <-kv.done:
			kv.Lock()
			DPrintf("kvserver %d-%d pullconfigure quited\n",kv.gid, kv.me)
			kv.Unlock()
			return
		default:
		}
		kv.Lock()
		//only when a config is applied will this ticker continue to fetch the new configuration
		if !kv.configApplied{
			kv.Unlock()
			time.Sleep(20*time.Millisecond)
			continue
		}
		kv.Unlock()
		newConfig:=kv.ctrlClient.Query(kv.config.Num+1)
		//newConfig:=kv.ctrlClient.Query(-1)
		kv.Lock()
		kv.checkMaxSizeExceeded()
		//switching config 1 need to be treate specially
		if kv.config.Num==0&& newConfig.Num==1{
			DPrintf("server %d-%d configure fetched %v\n",kv.gid,kv.me,newConfig)
			//create a new map for each shard belong to this gid
			for i:=0;i<shardctrler.NShards;i++{
				if newConfig.Shards[i]==kv.gid{
					kv.kvMap.ImportShard(i,make(map[string]string))
				}
			}
			kv.configApplied=true
			kv.config=newConfig.Copy()
			if len(kv.config.Groups[kv.gid])!=0{

				kv.us=kv.config.Groups[kv.gid]
			}
			kv.Unlock()
			continue
		}

		if newConfig.Num!=1&& newConfig.Num>kv.config.Num{
			DPrintf("server %d-%d configure fetched %v\n",kv.gid,kv.me,newConfig)
			//a new config
			oldConfig:=kv.config
			kv.newConfig=newConfig.Copy()
			kv.configApplied=false

			//kick out all maps that need to be loaded
			for i:=0;i<shardctrler.NShards;i++{
				//shards that should transfer to us
				// if oldConfig.Shards[i]!=kv.gid && kv.newConfig.Shards[i]==kv.gid{
				// 	//kv.kvMap.DeleteShard(i)
				// }
			}
			DPrintf("kvserver %d-%d started fetching missing Shard\n",kv.gid,kv.me)
			go kv.fetchMissingShard(oldConfig)
			
		}
		kv.Unlock()
		time.Sleep(20*time.Millisecond)
	}
}

func (kv *ShardKV)fetchMissingShard(oldConfig shardctrler.Config){
		kv.Lock()
	missingShards:=make(map[int][]int)
	for gid,_:=range oldConfig.Groups{
		if gid==kv.gid{
			continue
		}
		for i:=0;i<shardctrler.NShards;i++{
			if oldConfig.Shards[i]==gid && kv.newConfig.Shards[i]==kv.gid{
				if _,exist:=missingShards[gid];!exist{
					missingShards[gid]=make([]int, 0)
				}
				missingShards[gid]=append(missingShards[gid], i)
			}
		}
	}
	DPrintf("kvserver %d-%d  missing Shard are  %v \n\t new config is %v\n\t old config is %v\n",kv.gid,kv.me,missingShards,kv.newConfig,oldConfig)
	// if len(missingShards)==0{
	// 	DPrintf("here0604\n")
	// 	kv.configApplied=true
	// 	DPrintf("kvserver %d-%d  missing Shard finish ,current state %s \n",kv.gid,kv.me,kv)
	// 	kv.Unlock()
	// 	return
	// }
	kv.Unlock()

	var wg sync.WaitGroup
	var allShardsGot =make(map[int]map[string]string)
	for gid1,shards1:=range missingShards{
		wg.Add(1)
		var servers=make([]string,len(oldConfig.Groups[gid1]))
		copy(servers,oldConfig.Groups[gid1])
		go func(gid int,shards []int){
			kv.Lock()
			client:=MakeClerk(kv.ctrlers,kv.make_end)
			
			args:=GetShardsArgs{
				Shards: shards,
				Gid: gid,
				Num: kv.newConfig.Num,
				Nonce: nrand(),
				Servers: servers,
			}
			DPrintf("kvserver %d-%d  sending getshardsargs %v\n",kv.gid,kv.me,args)
			kv.Unlock()

			reply:=client.GetShards(&args)

			kv.Lock()
			DPrintf("kvserver %d-%d get Shard  %v \n",kv.gid,kv.me,reply)
			if reply.Err==OK{
				for k,v:=range reply.Data{
					allShardsGot[k]=v
				}
			}
			kv.Unlock()
			wg.Done()
		}(gid1,shards1)
	}
	wg.Wait()
	
	kv.Lock()
	
	var us=make([]string,len(kv.newConfig.Groups[kv.gid]))
	copy(us,kv.newConfig.Groups[kv.gid])
	if len(us)==0{
		us=make([]string, len(kv.us))
		copy(us,kv.us)
	}
	if len(missingShards)==0 && len(us)==0{
		kv.configApplied=true
		kv.config=kv.newConfig
		if len(kv.config.Groups[kv.gid])!=0{

			kv.us=kv.config.Groups[kv.gid]
		}
		DPrintf("kvserver %d-%d  missing Shard finish ,current state %s \n",kv.gid,kv.me,kv)
		kv.Unlock()
		return
	}
	args2:=InstallShardArgs{
		Data: allShardsGot,
		Num: kv.newConfig.Num,
		Nonce:int64(ihash(fmt.Sprintf("gid-%d-fetch-%d",kv.gid,kv.newConfig.Num))),
		Servers: us,
		Config: kv.newConfig.Copy(),
	}
	client:=MakeClerk(kv.ctrlers,kv.make_end)	
	kv.Unlock()

	client.InstallShards(&args2)

	kv.Lock()
	DPrintf("kvserver %d-%d  missing Shard finish ,current state %s \n",kv.gid,kv.me,kv)
	kv.Unlock()


}