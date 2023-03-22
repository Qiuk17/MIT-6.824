package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type JobStatus int

const (
	NotAssigned JobStatus = iota
	Running
	Done
)

type JobState struct {
	Type       JobType
	Key        string
	Status     JobStatus
	AssignedTo *WorkerState
}

type WorkerState struct {
	WorkerId     int
	AssignedTask *JobState
}

type Coordinator struct {
	// Your definitions here.
	workerIdCnt int

	Jobs map[int]*JobState

	workersMutex sync.Mutex
	Workers      map[int]*WorkerState
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RegisterWorker(req RegisterWorkerRequest, resp *RegisterWorkerResponse) error {
	c.workersMutex.Lock()
	defer c.workersMutex.Unlock()

	c.Workers[c.workerIdCnt] = &WorkerState{
		WorkerId:     c.workerIdCnt,
		AssignedTask: nil,
	}
	resp.WorkerId = c.workerIdCnt
	c.workerIdCnt = c.workerIdCnt + 1

	return nil
}

func (c *Coordinator) GetJob(req GetJobRequest, resp *GetJobResponse) error {

}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
