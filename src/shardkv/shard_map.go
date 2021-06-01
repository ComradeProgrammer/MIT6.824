package shardkv
type ShardMap struct{
	Map map[int]map[string]string
}

func NewShardMap()ShardMap{
	var res= ShardMap{
		Map: make(map[int]map[string]string),
	}
	return res
}

func (s *ShardMap)Get(key string)(string,bool){
	shard := key2shard(key)
	if _,exist:=s.Map[shard];!exist{
		return "",false
	}
	return s.Map[shard][key],true
}

func(s *ShardMap)Put(key string, value string){
	shard := key2shard(key)
	if _,exist:=s.Map[shard];!exist{
		s.Map[shard]=make(map[string]string)
	}
	s.Map[shard][key]=value
}

func (s*ShardMap)Append(key string, value string){
	shard := key2shard(key)
	if _,exist:=s.Map[shard];!exist{
		s.Map[shard]=make(map[string]string)
	}
	if v,exist:=s.Map[shard][key];!exist{
		s.Map[shard][key]=value
	}else{
		s.Map[shard][key]=v+value
	}
}

func (s *ShardMap)ExportShard(shard int)map[string]string{
	var res =make(map[string]string)
	for k,v:=range s.Map[shard]{
		res[k]=v
	}
	delete(s.Map,shard)
	return res
}

func (s* ShardMap)ImportShard(shard int,content map[string]string){
	s.Map[shard]=make(map[string]string)
	for k,v:=range content{
		s.Map[shard][k]=v
	}
}
func(s* ShardMap)Copy()ShardMap{
	var res ShardMap
	for k1,v1:=range s.Map{
		res.Map[k1]=make(map[string]string)
		for k2,v2:=range v1{
			res.Map[k1][k2]=v2
		}
	}
	return res
}