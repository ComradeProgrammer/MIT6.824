package kvraft
import (
	"log"
)
const DEBUG = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}
