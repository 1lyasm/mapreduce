package main

import (
	"fmt"
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

func makeWorker(id int) Worker {
	return Worker{Id: id, LastSeen: time.Now()}
}

func (workers *Workers) RegWorker(arg RegWorkerArg, reply *RegWorkerReply) error {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	var newId int
	if len(workers.WorkerList) >= 1 {
		newId = workers.WorkerList[len(workers.WorkerList)-1].Id + 1
	} else {
		newId = 0
	}
	workers.WorkerList = append(workers.WorkerList, makeWorker(newId))
	fmt.Println("after registering: ", workers.WorkerList)
	return nil
}

func main() {
	fileNames := os.Args[1:]
	fmt.Println(fileNames)
	workers := new(Workers)
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
