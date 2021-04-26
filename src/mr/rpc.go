package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobRequestArgs struct{
}
type JobRequestReply struct{
	HasJob bool
	JobID int
	ReduceNumber int //in map,this is reduce number ,in reduce,this is reduce index 
	JobType string//map or reduce
	Filename string//file name to process in map
	Filenames []string// file names to process in reduce
	
}

type JobFinishArgs struct{
	JobID int
	JobType string//map or reduce
}

type JobFinishReply struct{

}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
