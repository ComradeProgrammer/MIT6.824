package shardkv

import (
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
			//a new config
			kv.config=newConfig.Copy()
			kv.configApplied=true
		}
		kv.Unlock()
		time.Sleep(20*time.Millisecond)
	}
}