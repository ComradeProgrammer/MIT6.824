package kvraft

import "encoding/json"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET string="Get"
	APPEND string="Append"
	PUT string="Put"
)
type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID int64
}

func(p PutAppendArgs)String()string{
	data,_:=json.Marshal(p)
	return string(data)
}

type PutAppendReply struct {
	Err Err
}
func(p PutAppendReply)String()string{
	data,_:=json.Marshal(p)
	return string(data)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID int64
}
func(p GetArgs)String()string{
	data,_:=json.Marshal(p)
	return string(data)
}

type GetReply struct {
	Err   Err
	Value string
}
func(p GetReply)String()string{
	data,_:=json.Marshal(p)
	return string(data)
}