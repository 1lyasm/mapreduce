package worker

import (
	"log"
	"mapreduce/common"
	"net/rpc"
)

func main() {
	client, e := rpc.DialHTTP("tcp", common.IpAddr+":"+common.Port)
	if e != nil {
		log.Fatal(common.MakeFailMsg("rpc.DialHTTP"))
	}
}
