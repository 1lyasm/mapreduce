package coordinator

import (
	"fmt"
	"log"
	"mapreduce/common"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Task struct {
	File     string
	State    int
	WorkerId int
}

type Tasks struct {
	TaskList []Task
}

type ArgType int
type ReplyType int

func (tasks *Tasks) Demo(argType ArgType, replyType *ReplyType) error {
	fmt.Printf("hooy\n")
	return nil
}

func main() {
	fileNames := os.Args[1:]
	fmt.Println(fileNames)
	tasks := new(Tasks)
	rpc.Register(tasks)
	rpc.HandleHTTP()
	listener, e := net.Listen("tcp", common.IpAddr+":"+common.Port)
	if e != nil {
		log.Fatal(common.MakeFailMsg("net.Listen"))
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		http.Serve(listener, nil)
		wg.Done()
	}()
	wg.Wait()
}
