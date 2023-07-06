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

type KeyVal struct {
	Key string
	Val string
}

func regWorker(client *rpc.Client) int {
	arg := &RegWorkerArg{}
	reply := RegWorkerRep{}
	e := client.Call("Workers.RegWorker", arg, &reply)
	if e != nil {
		Fail("regWorker: client.Call", e)
	}
	log.Printf("regWorker: registered, id: %d", reply.Id)
	return reply.Id
}

func updateLastSeen(client *rpc.Client, id int) int {
	arg := &UpdateLastSeenArg{Id: id, LastSeen: time.Now()}
	reply := UpdateLastSeenRep{}
	e := client.Call("Workers.UpdateLastSeen", arg, &reply)
	if e != nil {
		Fail("updateLastSeen: client.Call", e)
	}
	if reply.Code == 1 {
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

func doTask(cl *rpc.Client, id int) int {
	arg, rep := &GetTaskArg{Id: id}, GetTaskRep{}
	e := cl.Call("Workers.GetTask", arg, &rep)
	if e != nil {
		Fail("doTask: cl.Call", e)
	}
	if rep.Code == 1 {
		return 1
	}
	bytes, e := os.ReadFile(rep.File)
	if e != nil {
		Fail("doTask: os.ReadFile", e)
	}
	f := string(bytes[:])
	log.Printf("f: %s", f)
	return 0
}

func doTasks(cl *rpc.Client, id int) {
	for {
		e := doTask(cl, id)
		if e == 1 {
			return
		}
	}
}

func incAtom(a *int, mu *sync.Mutex) {
	mu.Lock()
	*a = *a + 1
	mu.Unlock()
}

func main() {
	var pluFile string
	if len(os.Args) < 2 || os.Args[1] != "-v" {
		log.SetOutput(io.Discard)
		pluFile = os.Args[1]
	} else {
		pluFile = os.Args[2]
	}
	mapf, reducef := loadPlu(pluFile)
	log.Printf("mapf: %v, reducef: %v", mapf, reducef)
	cl, e := rpc.DialHTTP("tcp", HostIp+":"+Port)
	if e != nil {
		Fail("main: rpc.DialHTTP", e)
	}
	id := regWorker(cl)
	ch := make(chan int)
	doneCnt := 0
	muDone := sync.Mutex{}
	go func() {
		heartbeat(cl, id)
		log.Print("main: heartbeat error, restarting worker")
		ch <- 1
		incAtom(&doneCnt, &muDone)
	}()
	go func() {
		doTasks(cl, id)
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
