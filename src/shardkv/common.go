package shardkv

import (
	"encoding/json"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfigNum="ErrWrongConfigNum"
)
const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
	GETSHARDS="GetShards"
	INSTALLSHARDS="InstallShards"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Nonce int64
	Num int
}

func (r PutAppendArgs) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type PutAppendReply struct {
	Err Err
}

func (r PutAppendReply) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Nonce int64
	Num int
}

func (r GetArgs) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type GetReply struct {
	Err   Err
	Value string
}

func (r GetReply) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type GetShardsArgs struct{
	Shards []int
	Gid int
	Num int
	Nonce int64
	Servers []string
}
func (r GetShardsArgs) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}
type GetShardsReply struct{
	Err Err
	Data map[int]map[string]string
	ShardNonce map[int]map[int64]struct{}
}
func (r GetShardsReply) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type InstallShardArgs  struct{
	Data map[int]map[string]string
	ShardNonce map[int]map[int64]struct{}
	Config shardctrler.Config
	Num int
	Nonce int64
	Servers []string
}
func (r InstallShardArgs) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}

type InstallShardReply struct{
	Err Err
}
func (r InstallShardReply) String() string {
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}

}