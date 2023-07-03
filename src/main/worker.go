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

func callRegWorker(client *rpc.Client) int {
	arg := &RegWorkerArg{}
	reply := RegWorkerReply{}
	e := client.Call("Workers.RegWorker", arg, &reply)
	if e != nil {
		Fail("client.Call", e)
	}
	log.Printf("callRegWorker")
	return reply.Id
}

func callUpdateLastSeen(client *rpc.Client, id int) {
	arg := &UpdateLastSeenArg{Id: id, LastSeen: time.Now()}
	reply := UpdateLastSeenReply{}
	e := client.Call("Workers.UpdateLastSeen", arg, &reply)
	if e != nil {
		Fail("client.Call", e)
	}
	log.Printf("callUpdateLastSeen")
}

func sendHeartbeat(client *rpc.Client, id int) {
	for {
		callUpdateLastSeen(client, id)
		time.Sleep(time.Second)
	}
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "-v" {
		log.SetOutput(io.Discard)
	}
	client, e := rpc.DialHTTP("tcp", HostIp+":"+Port)
	if e != nil {
		Fail("rpc.DialHTTP", e)
	}
	Id := callRegWorker(client)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go sendHeartbeat(client, Id)
	wg.Wait()
}
