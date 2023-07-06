package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskFree = 0
	TaskLive = 1
	TaskDone = 2
)

const (
	TaskM = 0
	TaskR = 1
)

type Worker struct {
	Id   int
	Last time.Time
}

type Workers struct {
	mu   sync.Mutex
	List []Worker
}

type Task struct {
	File string
	Stat int
	Type int
}

type Tasks struct {
	List []Task
}

type Coordinator struct {
	workers *Workers
	tasks   *Tasks
	nRed    int
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

func (tasks *Tasks) fill(files []string) {
	for i := 0; i < len(files); i += 1 {
		tasks.List = append(tasks.List,
			Task{File: files[i], Stat: TaskFree, Type: TaskM})
	}
}

func (workers *Workers) Str() string {
	output := ""
	for i := 0; i < len(workers.List); i += 1 {
		output += fmt.Sprintf("%d ", workers.List[i].Id)
	}
	return fmt.Sprintf("workers: [ %s]", output)
}

func (c *Coordinator) clean() {
	period, timeout := time.Duration(time.Second), time.Duration(10*time.Second)
	for {
		c.workers.mu.Lock()
		now := time.Now()
		for i := 0; i < len(c.workers.List); i += 1 {
			if now.Sub(c.workers.List[i].Last).Milliseconds() >
				(timeout - period).Milliseconds() {
				log.Printf("clean: %d", c.workers.List[i].Id)
				c.workers.List[i] = c.workers.List[len(c.workers.List)-1]
				c.workers.List = c.workers.List[:len(c.workers.List)-1]
			}
			if len(c.workers.List) == 0 {
				break
			}
		}
		c.workers.mu.Unlock()
		log.Printf("clean: %s", c.workers.Str())
		time.Sleep(period)
	}
}

func (c *Coordinator) Heartb(arg *HbArg, rep *HbRep) error {
	c.workers.mu.Lock()
	defer c.workers.mu.Unlock()
	rep.Code = 0
	var w *Worker
	w = nil
	for i := 0; i < len(c.workers.List); i += 1 {
		if c.workers.List[i].Id == arg.Id {
			w = &c.workers.List[i]
		}
	}
	if w == nil {
		rep.Code = 1
	} else {
		w.Last = arg.Last
		log.Printf("Heartb: %d", arg.Id)
	}
	return nil
}

func (workers *Workers) maxId() int {
	max := -1
	for i := 0; i < len(workers.List); i += 1 {
		if workers.List[i].Id > max {
			max = workers.List[i].Id
		}
	}
	return max
}

func (c *Coordinator) RegW(arg *RegWArg, rep *RegWRep) error {
	c.workers.mu.Lock()
	defer c.workers.mu.Unlock()
	var newId int
	if len(c.workers.List) >= 1 {
		newId = c.workers.maxId() + 1
	} else {
		newId = 0
	}
	c.workers.List = append(c.workers.List, Worker{Id: newId, Last: time.Now()})
	rep.Id = newId
	rep.NRed = c.nRed
	log.Printf("RegW: %s, nRed: %d", c.workers.Str(), rep.NRed)
	return nil
}

func (c *Coordinator) GetT(arg *GetTArg, rep *GetTRep) error {
	rep.Code = 1
	for _, t := range c.tasks.List {
		if t.Stat == TaskFree {
			rep.Code = 0
			rep.File = t.File
			rep.Type = t.Type
			break
		}
	}
	return nil
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.tasks = new(Tasks)
	c.tasks.fill(files)
	c.workers = new(Workers)
	c.nRed = nReduce
	go c.clean()
	c.server()
	return &c
}
