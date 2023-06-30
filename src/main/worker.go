package main

import (
	"fmt"
	. "mapreduce/common"
	"net/rpc"
)

func callRegWorker(client *rpc.Client) {
	arg := &RegWorkerArg{}
	var reply RegWorkerReply
	e := client.Call("Workers.RegWorker", arg, &reply)
	if e != nil {
		Fail("client.Call", e)
	}
	fmt.Printf("added successfully\n")
}

func main() {
	client, e := rpc.DialHTTP("tcp", IpAddr+":"+Port)
	if e != nil {
		Fail("rpc.DialHTTP", e)
	}
	callRegWorker(client)
}
