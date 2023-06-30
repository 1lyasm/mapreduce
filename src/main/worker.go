package main

import (
	"fmt"
	"log"
	"mapreduce/common"
	"net/rpc"
)

func main() {
	client, e := rpc.DialHTTP("tcp", common.IpAddr+":"+common.Port)
	if e != nil {
		log.Fatal(common.MakeFailMsg("rpc.DialHTTP", e))
	}
	args := &common.ArgType{Arg: 13}
	var reply common.ReplyType
	e = client.Call("Tasks.Demo", args, &reply)
	if e != nil {
		log.Fatal(common.MakeFailMsg("client.Call", e))
	}
	fmt.Printf("reply: %d\n", reply.Reply)
}
