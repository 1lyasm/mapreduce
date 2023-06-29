package worker

import (
	"mapreduce/common"
	"net/rpc"
)

func main() {
	client, e := rpc.DialHTTP("tcp", "127.0.0.1"+common.Port)
}
