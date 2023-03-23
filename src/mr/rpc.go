package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type RegisterWorkerRequest struct {
}

type RegisterWorkerResponse struct {
	WorkerId int
}

type JobType int

const (
	JobMap JobType = iota
	JobReduce
	JobNothing
	JobExit
)

type GetJobRequest struct {
	WorkerId int
}

type GetJobResponse struct {
	Type JobType
	Key  string

	// type JobMap only
	NumReduce int

	// type JobReduce only
	WorkerIds []int
}

type GetWorkerIdRunMapJobsRequest struct {
}

type GetWorkerIdRunMapJobsResponse struct {
}

type JobDoneNotification struct {
	WorkerId int
	Type     JobType
	Key      string
}

type JobDoneNotificationResponse struct {
	Ok bool
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
