package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.
type CallArgs struct{
	TaskIndex int
	TaskType int
	IntermediateFileNames []string
}
type CallReply struct{
	TaskType int
	TaskIndex int
	FileName string
	NReduce int
	IntermediateFileNames []string

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
	}
