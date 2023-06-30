package main

import (
	"fmt"
	. "mapreduce/common"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Task struct {
	File     string
	State    int
	WorkerId int
}

type Tasks struct {
	TaskList []Task
}

type Workers struct {
	mu        sync.Mutex
	WorkerIds []int
}

func (workers *Workers) RegWorker(arg RegWorkerArg,
	reply *RegWorkerReply) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if len(workers.WorkerIds) >= 1 {
		workers.WorkerIds = append(workers.WorkerIds,
			workers.WorkerIds[len(workers.WorkerIds)-1]+1)
	} else {
		workers.WorkerIds = []int{0}
	}
	fmt.Println("after registering: ", workers.WorkerIds)
	return nil
}

func main() {
	fileNames := os.Args[1:]
	fmt.Println(fileNames)
	tasks := new(Tasks)
	workers := new(Workers)
	rpc.Register(tasks)
	rpc.Register(workers)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", IpAddr+":"+Port)
	if e != nil {
		Fail("net.Listen", e)
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		http.Serve(listener, nil)
		wg.Done()
	}()
	wg.Wait()
}
