package main

import (
	"io"
	"log"
	. "mapreduce/common"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
	"time"
)

func regWorker(client *rpc.Client) int {
	arg := &RegWorkerArg{}
	reply := RegWorkerReply{}
	e := client.Call("Workers.RegWorker", arg, &reply)
	if e != nil {
		Fail("regWorker: client.Call", e)
	}
	log.Printf("regWorker: registered, id: %d", reply.Id)
	return reply.Id
}

func updateLastSeen(client *rpc.Client, id int) int {
	arg := &UpdateLastSeenArg{Id: id, LastSeen: time.Now()}
	reply := UpdateLastSeenReply{}
	e := client.Call("Workers.UpdateLastSeen", arg, &reply)
	if e != nil {
		Fail("updateLastSeen: client.Call", e)
	}
	if reply.ErrorCode == 1 {
		return 1
	}
	log.Printf("updateLastSeen: updated")
	return 0
}

func heartbeat(client *rpc.Client, id int) {
	for {
		e := updateLastSeen(client, id)
		if e == 1 {
			return
		}
		time.Sleep(time.Second)
	}
}

func getTasks() {
}

func incAtom(a *int, mu *sync.Mutex) {
	mu.Lock()
	*a = *a + 1
	mu.Unlock()
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "-v" {
		log.SetOutput(io.Discard)
	}
	client, e := rpc.DialHTTP("tcp", HostIp+":"+Port)
	if e != nil {
		Fail("main: rpc.DialHTTP", e)
	}
	id := regWorker(client)
	ch := make(chan int)
	doneCnt := 0
	muDone := sync.Mutex{}
	go func() {
		heartbeat(client, id)
		log.Print("main: heartbeat error, restarting worker")
		ch <- 1
		incAtom(&doneCnt, &muDone)
	}()
	go func() {
		getTasks()
		log.Print("main: completed tasks")
		ch <- 2
		incAtom(&doneCnt, &muDone)
	}()
	muDone.Lock()
	done := <-ch
	if doneCnt == 2 {
		done = <-ch
	}
	muDone.Unlock()
	if done == 1 {
		cmd := exec.Command("/usr/bin/bash", "-c", "./worker")
		e = cmd.Start()
		if e != nil {
			Fail("main: Start", e)
		}
	}
}
