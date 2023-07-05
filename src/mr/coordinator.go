package main

import (
	"fmt"
	"io"
	"log"
	. "mapreduce/common"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Worker struct {
	Id       int
	LastSeen time.Time
}

type Workers struct {
	mu         sync.Mutex
	WorkerList []Worker
}

type Task struct {
	File string
}

type Tasks struct {
	List []Task
}

func (tasks *Tasks) fillTasks(files []string) {
	for i := 0; i < len(files); i += 1 {
		tasks.List = append(tasks.List, Task{File: files[i]})
	}
}

func (workers *Workers) String() string {
	output := ""
	for i := 0; i < len(workers.WorkerList); i += 1 {
		output += fmt.Sprintf("%d ", workers.WorkerList[i].Id)
	}
	return fmt.Sprintf("workers: [ %s]", output)
}

func makeWorker(id int) Worker {
	return Worker{Id: id, LastSeen: time.Now()}
}

func (workers *Workers) getMaxId() int {
	maxId := -1
	for i := 0; i < len(workers.WorkerList); i += 1 {
		if workers.WorkerList[i].Id > maxId {
			maxId = workers.WorkerList[i].Id
		}
	}
	return maxId
}

func (workers *Workers) RegWorker(arg RegWorkerArg, reply *RegWorkerRep) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	var newId int
	if len(workers.WorkerList) >= 1 {
		newId = workers.getMaxId() + 1
	} else {
		newId = 0
	}
	workers.WorkerList = append(workers.WorkerList, makeWorker(newId))
	reply.Id = newId
	log.Printf("RegWorker: %s", workers.String())
	return nil
}

func findWorkerById(workers *Workers, id int) *Worker {
	for i := 0; i < len(workers.WorkerList); i += 1 {
		if workers.WorkerList[i].Id == id {
			return &workers.WorkerList[i]
		}
	}
	return nil
}

func (workers *Workers) UpdateLastSeen(arg UpdateLastSeenArg,
	reply *UpdateLastSeenRep) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	reply.Code = 0
	seenWorker := findWorkerById(workers, arg.Id)
	if seenWorker == nil {
		reply.Code = 1
	} else {
		seenWorker.LastSeen = arg.LastSeen
		log.Printf("UpdateLastSeen: %d", arg.Id)
	}
	return nil
}

func secToMilli(s int) int {
	return s * 1000
}

func cleanWorker(workers *Workers, periodSec int, timeoutSec int) int {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	now := time.Now()
	for i := 0; i < len(workers.WorkerList); i += 1 {
		if int(now.Sub(workers.WorkerList[i].LastSeen).Milliseconds()) >
			secToMilli(timeoutSec-periodSec) {
			log.Printf("cleanWorker: cleaning %d", workers.WorkerList[i].Id)
			workers.WorkerList[i] = workers.WorkerList[len(workers.WorkerList)-1]
			workers.WorkerList = workers.WorkerList[:len(workers.WorkerList)-1]
			if len(workers.WorkerList) == 0 {
				return 1
			}
		}
	}
	log.Printf("cleanWorker: %s", workers.String())
	return 0
}

func cleanWorkerPeriodic(workers *Workers, periodSec int, timeoutSec int) {
	for {
		cleanWorker(workers, periodSec, timeoutSec)
		dur, e := time.ParseDuration(fmt.Sprintf("%d", periodSec) + "s")
		if e != nil {
			Fail("time.ParseDuration", e)
		}
		time.Sleep(dur)
	}
}

func (workers *Workers) GetTask(arg GetTaskArg, rep *GetTaskRep) error {
	rep.Code = 0
	return nil
}

func main() {
	var files []string
	if len(os.Args) >= 2 && os.Args[1] == "-v" {
		files = os.Args[2:]
	} else {
		log.SetOutput(io.Discard)
		files = os.Args[1:]
	}
	log.Printf("main: files: %s", files)
	workers := new(Workers)
	rpc.Register(workers)
	tasks := new(Tasks)
	tasks.fillTasks(files)
	rpc.Register(tasks)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", HostIp+":"+Port)
	if e != nil {
		Fail("main: net.Listen", e)
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		http.Serve(listener, nil)
		wg.Done()
	}()
	periodSec, timeoutSec := 1, 10
	go cleanWorkerPeriodic(workers, periodSec, timeoutSec)
	wg.Wait()
}
