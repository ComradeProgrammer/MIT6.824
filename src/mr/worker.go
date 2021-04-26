package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type SortKey []KeyValue

// for sorting by key.
func (a SortKey) Len() int           { return len(a) }
func (a SortKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for{
		//step 1,ask coordinator for a job
		ret,job:=callForJob()
		if !ret{
			//if something goes wrong,exit
			break
		}
		if !job.HasJob{
			//if there is no job,wait for a while and then ask for job again
			time.Sleep(time.Second)
			continue
		}
		if job.JobType=="map"{
			startMap(job,mapf)

		}else if job.JobType=="reduce"{
			startReduce(job,reducef)
		}else{
			fmt.Println("Worker: error,unrecognized job type")
		}
	}

}

func callForJob()(bool,JobRequestReply){
	args:=JobRequestArgs{}
	reply:=JobRequestReply{}
	ret:=call("Coordinator.GetJob",&args,&reply)
	return ret,reply
}
func callForJobFinish(jobtype string,jobID int){
	args:=JobFinishArgs{
		JobID: jobID,
		JobType: jobtype,
	}
	reply:=JobFinishReply{}
	call("Coordinator.JobDone",&args,&reply)

}

func startMap(job JobRequestReply,mapFunction func(string, string) []KeyValue){
	//read raw file 
	file, err := os.Open(job.Filename)
	if err!=nil{
		fmt.Printf("startMap: error when open file %s\n",job.Filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("startMap: error when read file %s\n",job.Filename)
	}
	//start mapjob
	kva := mapFunction(job.Filename, string(content))
	sort.Sort(SortKey(kva))
	//generate  intermediate files
	var outputFiles []*os.File=make([]*os.File, job.ReduceNumber)
	var outputArrays[][]KeyValue=make([][]KeyValue,job.ReduceNumber)
	for i:=0;i<job.ReduceNumber;i++{
		outputName := fmt.Sprintf("mr-%d-%d",job.JobID,i)
		outputfile, _ := os.Create(outputName)
		outputFiles[i]=outputfile
		defer outputfile.Close()

		outputArrays[i]=make([]KeyValue, 0)
	}
	//write each key-value pair to corresponding array
	for _,kv:=range kva{
		hash:=ihash(kv.Key)
		//fmt.Fprintf(outputFiles[hash%job.ReduceNumber],"%v %v\n",kv.Key,kv.Value)
		outputArrays[hash%job.ReduceNumber]=append(outputArrays[hash%job.ReduceNumber],kv)
	}
	//write to files
	for i:=0;i<len(outputArrays);i++{
		data,_:=json.Marshal(outputArrays[i])
		fmt.Fprint(outputFiles[i],string(data))
	}
	callForJobFinish("map",job.JobID)
}

func startReduce(job JobRequestReply,reduceFunction func(string, []string) string){
	//stole a lazy here, i know it's not good to use map here
	var kvs=make(map[string][]string)
	//read each file and add to kv map
	for _,filename:=range job.Filenames{
		file, err := os.Open(filename)
		if err!=nil{
			fmt.Printf("startReduce: error when open file %s\n",job.Filename)
		}
		defer file.Close()
		content, err := ioutil.ReadAll(file)
		var tmp=make([]KeyValue,0);
		_=json.Unmarshal(content,&tmp)
		for _,kv:=range tmp{
			kvs[kv.Key]=append(kvs[kv.Key], kv.Value)
		}
	}
	//open output file
	outputName := fmt.Sprintf("mr-out-%d",job.ReduceNumber)
	outputFile, _ := os.Create(outputName)
	defer outputFile.Close()
	//call reduce function on each key
	for k,vList:=range kvs{
		ret:=reduceFunction(k,vList)
		fmt.Fprintf(outputFile,"%v %v\n",k,ret)
	}
	callForJobFinish("reduce",job.JobID)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

