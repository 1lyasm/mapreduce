package main

import (
	"io"
	"log"
	. "mapreduce/common"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type HeartbeatStatus struct {
	mu   sync.Mutex
	code int
}

func (status *HeartbeatStatus) getCodeAtomic() int {
	status.mu.Lock()
	defer status.mu.Unlock()
	return status.code
}

func callRegWorker(client *rpc.Client) int {
	arg := &RegWorkerArg{}
	reply := RegWorkerReply{}
	e := client.Call("Workers.RegWorker", arg, &reply)
	if e != nil {
		Fail("callRegWorker: client.Call", e)
	}
	log.Printf("callRegWorker: registered, id: %d", reply.Id)
	return reply.Id
}

func callUpdateLastSeen(client *rpc.Client, id int) int {
	arg := &UpdateLastSeenArg{Id: id, LastSeen: time.Now()}
	reply := UpdateLastSeenReply{}
	e := client.Call("Workers.UpdateLastSeen", arg, &reply)
	if e != nil {
		Fail("callUpdateLastSeen: client.Call", e)
	}
	if reply.ErrorCode == 1 {
		return 1
	}
	log.Printf("callUpdateLastSeen: updated")
	return 0
}

func sendHeartbeat(client *rpc.Client, id int, wg *sync.WaitGroup,
	status *HeartbeatStatus) {
	for {
		e := callUpdateLastSeen(client, id)
		if e == 1 {
			log.Printf("sendHeartbeat: could not send")
			break
		}
		time.Sleep(time.Second)
	}
	status.code = 1
}

func requestTasks(wg *sync.WaitGroup) {
	wg.Done()
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "-v" {
		log.SetOutput(io.Discard)
	}
	status := HeartbeatStatus{code: 1}
	for status.getCodeAtomic() == 1 {
		client, e := rpc.DialHTTP("tcp", HostIp+":"+Port)
		if e != nil {
			Fail("rpc.DialHTTP", e)
		}
		Id := callRegWorker(client)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			status.mu.Lock()
			status.code = 0
			sendHeartbeat(client, Id, &wg, &status)
			status.mu.Unlock()
			log.Print("main: restarting worker")
			wg.Done()
		}()
		wg.Add(1)
		go requestTasks(&wg)
		wg.Wait()
	}
}
