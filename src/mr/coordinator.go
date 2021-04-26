package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	taskNo int

	reduceNumber int
	rawFiles []string
	newJobChannel chan JobRequestReply

	done bool

	jobMap map[int]JobRequestReply 
	jobChannels map[int]chan struct{}
	finishedJobs []int
	jobStage string
	jobStatusLock sync.Mutex

	


}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *JobRequestArgs, reply *JobRequestReply) error {
	select{
	case newJob:=<-c.newJobChannel:
		//have a new job
		*reply=newJob
		//start timer
		newJobChannel:=make(chan struct{},1)
		newJobTimerChannel:=time.After(10*time.Second)
		c.jobStatusLock.Lock()
		c.jobChannels[newJob.JobID]=newJobChannel
		c.jobStatusLock.Unlock()
		go c.Daemon(newJob.JobID,newJobChannel,newJobTimerChannel)
		//fmt.Printf("No.%d job is fetched.job is %v\n",newJob.JobID,newJob)
	default:
		//nojob
		*reply=JobRequestReply{
			HasJob: false,
		}
	}
	return nil
}

func (c *Coordinator)JobDone(args *JobFinishArgs, reply *JobFinishArgs) error {
	c.jobStatusLock.Lock()
	defer c.jobStatusLock.Unlock()
	//fmt.Printf("No.%d job jobdone is called\n",args.JobID)
	channel,ok:=c.jobChannels[args.JobID]
	if !ok{
		// non-exist job,abandon
		return nil
	}
	//inform the daemon
	channel<-struct{}{}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.jobStatusLock.Lock()
	defer c.jobStatusLock.Unlock()

	// Your code here.


	return c.done
}
func (c *Coordinator)Daemon(id int,jobChannel chan struct{},timerChannel <-chan time.Time) {
	select{
	case <-jobChannel:
		//the job is done
		//fmt.Printf("No.%d job is done\n",id)
		c.FinishJob(id)
	case <-timerChannel:
		//the old worker is dead,reset it as an new job
		//fmt.Printf("No.%d job time expired\n",id)
		c.RestartJob(id)
	}

}
func (c* Coordinator)FinishJob(id int){
	c.jobStatusLock.Lock()
	defer c.jobStatusLock.Unlock()
	//delete the channel in channelmap
	delete(c.jobChannels,id)
	//add to finished list
	c.finishedJobs=append(c.finishedJobs, id)
	//if all map jobs are finished
	if c.jobStage=="map"&&len(c.finishedJobs)==len(c.rawFiles){
		c.jobStage="reduce"
		for i:=0;i<c.reduceNumber;i++{
			c.StartReduceJob(i)
		}
		c.finishedJobs=make([]int, 0)
	}
	//finished
	if c.jobStage=="reduce"&&len(c.finishedJobs)==c.reduceNumber{
		c.done=true
	}
}

func (c* Coordinator)RestartJob(id int){
	c.jobStatusLock.Lock()
	defer c.jobStatusLock.Unlock()
	var newJob=c.jobMap[id]
	newJob.JobID=c.taskNo
	c.jobMap[newJob.JobID]=newJob
	c.newJobChannel<-newJob
	c.taskNo++
	//fmt.Printf("No.%d job is restarted as job No.%d\n",id,newJob.JobID)
}

//already have lock, no need to get lock
func (c* Coordinator)StartReduceJob(number int){
	var newJob JobRequestReply=JobRequestReply{
		HasJob: true,
		JobID: c.taskNo,
		ReduceNumber: number,
		JobType: "reduce",
		Filename: "",
		Filenames:make([]string, 0),
	}
	//tell worker the file he need to reduce
	for i:=0;i<len(c.finishedJobs);i++{
		fileName:=fmt.Sprintf("mr-%d-%d",c.finishedJobs[i],number)
		newJob.Filenames=append(newJob.Filenames, fileName)
	}
	c.jobMap[newJob.JobID]=newJob
	c.newJobChannel<-newJob
	c.taskNo++
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	bufferSize:=len(files)
	if nReduce>bufferSize{
		bufferSize=nReduce
	}
	c := Coordinator{
		taskNo: 0,
		reduceNumber: nReduce,
		rawFiles: files,
		jobMap: make(map[int]JobRequestReply),
		jobChannels: make(map[int]chan struct{}),
		newJobChannel: make(chan JobRequestReply,bufferSize),
		finishedJobs: make([]int,0),
		jobStage: "map",
		done: false,
	}
	//start dispatch job
	for _,file:=range files{
		//don't need lock here because nobody else modify it
		newJob:=JobRequestReply{
			HasJob: true,
			JobID: c.taskNo,
			ReduceNumber: nReduce,
			JobType: "map",
			Filename: file,
			Filenames: nil,
		}
		c.newJobChannel<-newJob
		c.jobMap[newJob.JobID]=newJob
		c.taskNo++
	}
	//fmt.Printf("Coordinator is made,reduce number %d\n",c.reduceNumber)
	//fmt.Printf("%d jobs in queue before start\n",len(c.newJobChannel))
	c.server()
	return &c
}
