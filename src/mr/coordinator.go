package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const JobQueueCap = 100
const JobMaxExecutionDuration = 10 * time.Second

type JobState struct {
	Type JobType
	Key  string
}

type Coordinator struct {
	nReduce int
	nMap    int

	mapWaitGroup    sync.WaitGroup
	reduceWaitGroup sync.WaitGroup

	workerIdCntMutex sync.Mutex
	workerIdCnt      int

	undoneJobsChan chan *JobState

	runningJobDonesChanMutex sync.Mutex
	runningJobDonesChan      map[string]chan interface{}

	workerIdsDoneMapJobsMutex sync.Mutex
	workerIdsDoneMapJobs      []int

	allDone bool
}

func (c *Coordinator) RegisterWorker(req RegisterWorkerRequest, resp *RegisterWorkerResponse) error {
	c.workerIdCntMutex.Lock()
	defer c.workerIdCntMutex.Unlock()

	resp.WorkerId = c.workerIdCnt
	c.workerIdCnt = c.workerIdCnt + 1

	log.Printf("worker(id=%d) registered", resp.WorkerId)
	return nil
}

func (c *Coordinator) GetJob(req GetJobRequest, resp *GetJobResponse) error {
	select {
	case job := <-c.undoneJobsChan:
		resp.Type = job.Type
		resp.Key = job.Key
		resp.NumReduce = c.nReduce
		if job.Type == JobReduce {
			log.Print(c.workerIdsDoneMapJobs)
			resp.WorkerIds = c.workerIdsDoneMapJobs
		}
		log.Printf("worker(id=%d) gets job(key=%s)", req.WorkerId, job.Key)

		c.runningJobDonesChanMutex.Lock()
		defer c.runningJobDonesChanMutex.Unlock()
		c.runningJobDonesChan[job.Key] = make(chan interface{})
		go c.watchRunningJob(job, req.WorkerId)

	default:
		resp.Type = JobNothing
	}

	return nil
}

func (c *Coordinator) NotifyJobDone(notification JobDoneNotification, resp *JobDoneNotificationResponse) error {
	c.runningJobDonesChanMutex.Lock()
	defer c.runningJobDonesChanMutex.Unlock()

	log.Printf("recv job() done notification")

	done, exists := c.runningJobDonesChan[notification.Key]
	if exists {
		close(done)
		resp.Ok = true
	} else {
		resp.Ok = false
	}
	return nil
}

func (c *Coordinator) initMapJobs(files []string) {
	for _, file := range files {
		c.undoneJobsChan <- &JobState{
			Type: JobMap,
			Key:  file,
		}
		log.Printf("add task(key=%s)", file)
		c.mapWaitGroup.Add(1)
	}

	// wait for all map jobs to be done
	go func() {
		c.mapWaitGroup.Wait()
		log.Print("map done. adding reduce jobs")
		c.initReduceJobs(c.nReduce)
	}()
}

func (c *Coordinator) initReduceJobs(nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.undoneJobsChan <- &JobState{
			Type: JobReduce,
			Key:  strconv.FormatInt(int64(i), 10),
		}
		log.Printf("add task(key=%d)", i)
		c.reduceWaitGroup.Add(1)
	}

	go func() {
		c.reduceWaitGroup.Wait()
		log.Print("reduce done. quitting")
		c.allDone = true
	}()
}

func (c *Coordinator) watchRunningJob(job *JobState, workerId int) {

	log.Printf("start to watch job(key=%s)", job.Key)

	select {
	case <-c.runningJobDonesChan[job.Key]:
		// job Done
		log.Printf("watch job(key=%s) done", job.Key)

		switch job.Type {
		case JobMap:
			c.workerIdsDoneMapJobsMutex.Lock()
			defer c.workerIdsDoneMapJobsMutex.Unlock()
			c.workerIdsDoneMapJobs = append(c.workerIdsDoneMapJobs, workerId)
			c.mapWaitGroup.Done()

		case JobReduce:
			c.reduceWaitGroup.Done()
		}

	case <-time.After(JobMaxExecutionDuration):
		// timeout
		c.undoneJobsChan <- job
		log.Printf("job(key=%s) timeout", job.Key)
	}

	c.runningJobDonesChanMutex.Lock()
	defer c.runningJobDonesChanMutex.Unlock()
	delete(c.runningJobDonesChan, job.Key)
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
	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetPrefix("[c] ")
	c := Coordinator{
		workerIdCnt:          0,
		nMap:                 len(files),
		nReduce:              nReduce,
		undoneJobsChan:       make(chan *JobState, JobQueueCap),
		workerIdsDoneMapJobs: make([]int, 0),
		runningJobDonesChan:  make(map[string]chan interface{}),
		allDone:              false,
	}
	c.initMapJobs(files)
	c.server()
	return &c
}
