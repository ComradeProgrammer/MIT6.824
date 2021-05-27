package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader=0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	i:=ck.lastLeader
	args:=GetArgs{
		Key: key,
		ID: nrand(),
	}
	reply:=GetReply{}
	for{
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok{
			DPrintf("Get request %s receives reply from server %s\n",args,reply)
			if reply.Err==ErrNoKey{
				ck.lastLeader=i
				return ""
			}else if reply.Err==OK{
				ck.lastLeader=i
				return reply.Value
			}
		}else{
			DPrintf("Get request %s to server  failed\n",args)
		}
		i=(i+1)%len(ck.servers)
		time.Sleep(10*time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	i:=ck.lastLeader
	arg:=PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ID:nrand(),
	}
	reply:=PutAppendReply{}
	for{
		ok := ck.servers[i].Call("KVServer.PutAppend", &arg, &reply)
		if ok{
			DPrintf("Put request %s receives reply from server  %s\n",arg,reply)
			if reply.Err==OK{
				ck.lastLeader=i
				return
			}
		}else{
			DPrintf("PutAppend request %s to server failed\n",arg)
		}
		i=(i+1)%len(ck.servers)
		time.Sleep(10*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
