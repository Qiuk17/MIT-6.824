package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const JobQueueCap int = 100

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

	workersMutex sync.Mutex
	Workers      map[int]*WorkerState

	UndoneJobsChan chan *JobState
}

func (c *Coordinator) RegisterWorker(req RegisterWorkerRequest, resp *RegisterWorkerResponse) error {
	c.workersMutex.Lock()
	defer c.workersMutex.Unlock()

	c.Workers[c.workerIdCnt] = &WorkerState{
		WorkerId:     c.workerIdCnt,
		AssignedTask: nil,
	}
	resp.WorkerId = c.workerIdCnt
	c.workerIdCnt = c.workerIdCnt + 1

	log.Printf("worker(id=%d) registered", resp.WorkerId)
	time.Sleep(5 * time.Second)
	return nil
}

func (c *Coordinator) GetJob(req GetJobRequest, resp *GetJobResponse) error {

	return nil
}

func (c *Coordinator) initMapJobs(files []string) {
	for _, file := range files {
		c.UndoneJobsChan <- &JobState{
			Type:       JobMap,
			Key:        file,
			Status:     NotAssigned,
			AssignedTo: nil,
		}

		log.Printf("add task(key=%s)", file)
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetPrefix("[c] ")
	c := Coordinator{
		workerIdCnt:    0,
		UndoneJobsChan: make(chan *JobState, JobQueueCap),
		Workers:        make(map[int]*WorkerState, 5),
	}
	c.initMapJobs(files)
	// Your code here.

	c.server()
	return &c
}
