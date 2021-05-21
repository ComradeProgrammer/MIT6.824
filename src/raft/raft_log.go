package raft

import "encoding/json"
type Log struct {
	Command interface{}
	Term    int
	Index 	int
}

func (l Log) String() string {
	data, _ := json.Marshal(l)
	return string(data)
}


type LogVector struct{
	Logs []Log
	PrevIndex int
	PrevLogTerm int
	RealLength int
}
func (l LogVector) String() string {
	data, _ := json.Marshal(l)
	return string(data)
}

func NewLogVector()LogVector{
	var tmp=LogVector{
		Logs:make([]Log, 0),
		PrevIndex: -1,
		RealLength: 0,
		PrevLogTerm: -1,
	}
	return tmp
}

func (v *LogVector)PushBack(l ...Log){
	v.Logs = append(v.Logs, l...)
	v.RealLength+=len(l)
}

func (v *LogVector)Get(index int)Log{
	return v.Logs[index-v.PrevIndex-1]
}
//3 4 5 6 prev=2 
/**
@brief will wipe out every log after no.Index log,not including No.index log
@param index index of the last consistent log
*/
func(v *LogVector)WipeOutInconsistentLogs(index int){
	v.Logs=v.Logs[0:index-v.PrevIndex]
	v.RealLength=index+1
}
/**
@brief will wipe out every log before No.index log, including No.index log
@param index index of last snapshotted log
*/
func(v *LogVector)WipeOutSnapshottedLogs(index int){
	v.PrevLogTerm=v.Get(index).Term
	v.Logs=v.Logs[index-v.PrevIndex:]
	v.PrevIndex=index
}

func(v *LogVector)ReplaceLogs(logs []Log,prevIndex,prevTerm,realLength int){
	v.Logs=logs
	v.PrevIndex=prevIndex
	v.RealLength=realLength	
	v.PrevLogTerm=prevTerm
}

/**
@brief return the real length of log, despite of snappshottedlogs 
*/
func(v *LogVector)Len()int{
	return v.RealLength
}

/**
@brief return the index of last snapshotted log
*/
func(v *LogVector)GetPrevIndex()int{
	return v.PrevIndex
}

/**

*/
func (v *LogVector)GetLastIndexAndterm()(int,int){
	if len(v.Logs)==0{
		return v.PrevIndex,v.PrevLogTerm
	}
	return v.Logs[len(v.Logs)-1].Index,v.Logs[len(v.Logs)-1].Term
}