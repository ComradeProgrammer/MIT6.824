package shardkv

import "time"
func (kv *ShardKV)pullConfiguration(){
	for{
		newConfig:=kv.ctrlClient.Query(-1)
		kv.Lock()
		if newConfig.Num>kv.config.Num{
			kv.config=newConfig
		}
		kv.Unlock()
		time.Sleep(20*time.Millisecond)
	}
}