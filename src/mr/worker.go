package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

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

func heartb(id int) {
	for {
		arg, rep := &HbArg{Id: id, Last: time.Now()}, HbRep{}
		e := call("Coordinator.Heartb", arg, &rep)
		if !e {
			Fail("heartb: call", nil)
		}
		if rep.Code == 1 {
			break
		}
		log.Printf("heartb: sent")
		time.Sleep(time.Second)
	}
}

func reg() int {
	arg, rep := &RegWArg{}, RegWRep{}
	e := call("Coordinator.RegW", arg, &rep)
	if !e {
		Fail("reg: call", nil)
	}
	log.Printf("reg: id: %d", rep.Id)
	return rep.Id
}

func doTask(id int, mapf func(string, string) []KeyValue, redf func(string, []string) string) {
	for {
		arg, rep := &GetTArg{Id: id}, GetTRep{}
		e := call("Coordinator.GetT", arg, &rep)
		if !e {
			Fail("doTask: call", nil)
		}
		if rep.Code == 1 {
			break
		}
		bytes, err := os.ReadFile(rep.File)
		if err != nil {
			Fail("doTask: os.ReadFile", err)
		}
		f := string(bytes[:])
		if rep.Type == TaskM {
			mapf("", f)
		}
	}
}

func incAtom(a *int, mu *sync.Mutex) {
	mu.Lock()
	*a = *a + 1
	mu.Unlock()
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RunW(mapf func(string, string) []KeyValue,
	redf func(string, []string) string) {
	id := reg()
	ch := make(chan int)
	doneCnt := 0
	muDone := sync.Mutex{}
	go func() {
		heartb(id)
		log.Print("RunW: heartb error, restarting worker")
		ch <- 1
		incAtom(&doneCnt, &muDone)
	}()
	go func() {
		doTask(id, mapf, redf)
		log.Print("RunW: completed tasks")
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
		RunW(mapf, redf)
	}
}
