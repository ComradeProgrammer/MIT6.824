package shardkv

import (
	"sync"
	"time"

	"6.824/shardctrler"
)
func (kv *ShardKV)pullConfiguration(){
	for{
		kv.Lock()
		//only when a config is applied will this ticker continue to fetch the new configuration
		if !kv.configApplied{
			kv.Unlock()
			time.Sleep(20*time.Millisecond)
			continue
		}
		kv.Unlock()
		newConfig:=kv.ctrlClient.Query(kv.config.Num+1)
		kv.Lock()
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
			kv.Unlock()
			continue
		}

		if newConfig.Num!=1&& newConfig.Num>kv.config.Num{
			DPrintf("server %d-%d configure fetched %v\n",kv.gid,kv.me,newConfig)
			//a new config
			oldConfig:=kv.config
			kv.config=newConfig.Copy()
			kv.configApplied=false

			//kick out all maps that need to be loaded
			for i:=0;i<shardctrler.NShards;i++{
				//shards that should transfer to us
				if oldConfig.Shards[i]!=kv.gid && kv.config.Shards[i]==kv.gid{
					kv.kvMap.DeleteShard(i)
				}
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
			if oldConfig.Shards[i]==gid && kv.config.Shards[i]==kv.gid{
				if _,exist:=missingShards[gid];!exist{
					missingShards[gid]=make([]int, 0)
				}
				missingShards[gid]=append(missingShards[gid], i)
			}
		}
	}
	DPrintf("kvserver %d-%d  missing Shard are  %v \n\t new config is %v\n\t old config is %v\n",kv.gid,kv.me,missingShards,kv.config,oldConfig)
	kv.Unlock()

	var wg sync.WaitGroup
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
				Num: kv.config.Num,
				Nonce: nrand(),
				Servers: servers,
			}
			DPrintf("kvserver %d-%d  sending getshardsargs %v\n",kv.gid,kv.me,args)
			kv.Unlock()
			reply:=client.GetShards(&args)
			kv.Lock()
			DPrintf("kvserver %d-%d get Shard  %v \n",kv.gid,kv.me,reply)
			if reply.Err==OK{
				for s,m:=range reply.Data{
					kv.kvMap.ImportShard(s,m)
				}
			}
			kv.Unlock()
			wg.Done()
		}(gid1,shards1)
	}
	wg.Wait()
	kv.Lock()
	DPrintf("kvserver %d-%d  missing Shard finish ,current state %s \n",kv.gid,kv.me,kv)
	kv.configApplied=true
	kv.Unlock()


}