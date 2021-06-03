package raft

import (
	"log"
	"sync"
)

var DEBUG bool =false
func DPrintf(format string, a ...interface{}) {
	log.SetFlags(log.Lmicroseconds)
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}

type ThreadSafeQueue struct{
	*sync.Cond 
	msgs []ApplyMsg
}
func NewThreadSafeQueue()(*ThreadSafeQueue){
	lock:=sync.Mutex{}
	var t =ThreadSafeQueue{
		Cond: sync.NewCond(&lock),
		msgs: make([]ApplyMsg, 0),
	}
	return &t
}

func (t *ThreadSafeQueue)Pushback(m ApplyMsg){
	t.Cond.L.Lock()
	t.msgs = append(t.msgs, m)
	t.Cond.L.Unlock()
	t.Cond.Broadcast()
}

func (t *ThreadSafeQueue)PopFront()ApplyMsg{
	t.Cond.L.Lock()
	for{
		if len(t.msgs)==0{
			t.Cond.Wait()
		}else{
			break
		}
	}
	m:=t.msgs[0]
	t.msgs=t.msgs[1:]
	t.Cond.L.Unlock()
	return m
}