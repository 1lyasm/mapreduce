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

func (workers *Workers) RegWorker(arg RegWorkerArg, reply *RegWorkerReply) error {
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
	reply *UpdateLastSeenReply) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	reply.ErrorCode = 0
	seenWorker := findWorkerById(workers, arg.Id)
	if seenWorker == nil {
		reply.ErrorCode = 1
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

func main() {
	var fileNames []string
	if len(os.Args) >= 2 && os.Args[1] == "-v" {
		fileNames = os.Args[2:]
	} else {
		log.SetOutput(io.Discard)
		fileNames = os.Args[1:]
	}
	log.Printf("fileNames: %s", fileNames)
	workers := new(Workers)
	rpc.Register(workers)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", HostIp+":"+Port)
	if e != nil {
		Fail("net.Listen", e)
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
