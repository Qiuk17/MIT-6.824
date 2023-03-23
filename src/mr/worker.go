package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"regexp"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

// main/mrworker.go calls this function.
func Worker(pluginName string) {

	mapf, _ := loadPlugin(pluginName)

	var registerResp RegisterWorkerResponse
	call("Coordinator.RegisterWorker", RegisterWorkerRequest{}, &registerResp)
	log.SetPrefix(fmt.Sprintf("[%d]", registerResp.WorkerId))
	log.Printf("get id %d", registerResp.WorkerId)

	for {
		var getJobResponse GetJobResponse
		call("Coordinator.GetJob", GetJobRequest(registerResp), &getJobResponse)

		switch getJobResponse.Type {
		case JobNothing:
			log.Print("nothing to do, sleep for 1 sec")
			time.Sleep(time.Second)
			continue
		case JobExit:
			log.Print("quitting")
			os.Exit(0)
		}

		log.Printf("get job(key=%s)", getJobResponse.Key)

		if getJobResponse.Type == JobMap {
			runMap(registerResp.WorkerId, getJobResponse.Key, getJobResponse.NumReduce, mapf)
		} else {
			runReduce(registerResp.WorkerId, getJobResponse.WorkerIds, getJobResponse.Key)
		}

		var nResp JobDoneNotificationResponse
		call("Coordinator.NotifyJobDone", JobDoneNotification{
			WorkerId: registerResp.WorkerId,
			Type:     getJobResponse.Type,
			Key:      getJobResponse.Key,
		}, &nResp)
		log.Printf("job(key=%s) done", getJobResponse.Key)
	}
}

func intermediateName(mapKey string, reduceKey int, workerId int) string {
	return fmt.Sprintf("temp-%d-%d-%s", workerId, reduceKey, mapKey)
}

func getIntermediateNames(reduceKey string, workerIds []int) []string {
	ret := make([]string, 0)
	log.Print(workerIds)
	workers := make(map[int]struct{})
	for id := range workerIds {
		workers[id] = struct{}{}
	}

	log.Printf("read map results(reduceKey=%s)", reduceKey)

	dirs, err := os.ReadDir("./")
	if err != nil {
		panic("cannot read dir")
	}
	for workerId := range workers {
		reg := regexp.MustCompile(fmt.Sprintf("temp-%d-%s.*", workerId, reduceKey))
		for _, dir := range dirs {
			if reg.Match([]byte(dir.Name())) {
				log.Print(dir.Name())
				ret = append(ret, dir.Name())
			}
		}
	}

	return ret
}

func runMap(workerId int, key string, nReduce int, mapf func(string, string) []KeyValue) error {

	content, err := os.ReadFile(key)
	if err != nil {
		return err
	}

	log.Printf("exec map(key=%s, contentLen=%d)", key, len(content))

	kva := mapf(key, string(content))

	outputFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		outputFiles[i], err = os.Create(intermediateName(key, i, workerId))
		if err != nil {
			return err
		}
	}

	for _, kv := range kva {
		_, err = fmt.Fprintf(outputFiles[ihash(kv.Key)%nReduce], "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}

	for _, file := range outputFiles {
		file.Close()
	}

	return nil
}

func runReduce(workerId int, mapWorkerIds []int, reduceKey string) {
	files := getIntermediateNames(reduceKey, mapWorkerIds)
	intermediates := []KeyValue{}

	for _, file := range files {
		log.Printf("reading file %s", file)
		ofile, _ := os.Open(file)
		for {
			kv := KeyValue{}
			_, err := fmt.Fscanln(ofile, &kv.Key, &kv.Value)
			if err != nil {
				break
			}
			intermediates = append(intermediates, kv)
		}
		ofile.Close()

		sort.Sort(ByKey(intermediates))
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
