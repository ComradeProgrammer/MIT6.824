package shardctrler

import "encoding/json"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const (
	OK = "OK"
	KILLED ="KILLED"
	WRONGLEADER = "WRONGLEADER"
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}
func (c*Config)Copy()Config{
	newConfig:=Config{
		Num: c.Num,
		Groups: make(map[int][]string),
	}
	for k,v:=range c.Groups{
		newConfig.Groups[k]=make([]string, len(v))
		copy(newConfig.Groups[k],v)
	}
	for i,v:=range c.Shards{
		newConfig.Shards[i]=v
	}
	return newConfig
}

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Nonce int64
}

func (r JoinArgs)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}
func (r JoinReply)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}

type LeaveArgs struct {
	GIDs []int
	Nonce int64
}
func (r  LeaveArgs)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}
func (r LeaveReply)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}


type MoveArgs struct {
	Shard int
	GID   int
	Nonce int64
}
func (r MoveArgs)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}


type MoveReply struct {
	WrongLeader bool
	Err         Err
}
func (r MoveReply)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}

type QueryArgs struct {
	Num int // desired config number
	Nonce int64
}
func (r QueryArgs)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}


type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
func (r QueryReply)String()string{
	if DEBUG {
		data, _ := json.Marshal(r)
		return string(data)
	} else {
		return ""
	}
}