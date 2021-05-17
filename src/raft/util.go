package raft

import (
	"log"
)

const DEBUG bool = false

func DPrintf(format string, a ...interface{}) {
	log.SetFlags(log.Lmicroseconds)
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}
