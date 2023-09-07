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
	mu sync.Mutex
	L  []Worker
}

type Task struct {
	File  string
	Stat  int
	Type  int
	Num   int
	Start time.Time
}

type Tasks struct {
	mu sync.Mutex
	L  []Task
}

type Coordinator struct {
	workers  *Workers
	tasks    *Tasks
	nRed     int
	muRedCnt sync.Mutex
	redCnt   int
	fCnt     int
	muKf     sync.Mutex
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.muRedCnt.Lock()
	defer c.muRedCnt.Unlock()
	return c.redCnt == c.nRed
}

func (tasks *Tasks) fill(files []string, nRed int) {
	for i := 0; i < len(files); i += 1 {
		log.Printf("fill: new task with file: %s", files[i])
		tasks.L = append(tasks.L,
			Task{File: files[i], Stat: TaskFree, Type: TaskM, Num: i})
	}
	for i := 0; i < nRed; i += 1 {
		tasks.L = append(tasks.L,
			Task{Stat: TaskFree, Type: TaskR,
				Num: i})
	}
}

func (workers *Workers) Str() string {
	output := ""
	for i := 0; i < len(workers.L); i += 1 {
		output += fmt.Sprintf("%d ", workers.L[i].Id)
	}
	return fmt.Sprintf("workers: [ %s]", output)
}

func (c *Coordinator) clean() {
	period, timeout := time.Duration(time.Second), time.Duration(10*time.Second)
	for {
		c.workers.mu.Lock()
		now := time.Now()
		for i := 0; i < len(c.workers.L); i += 1 {
			if now.Sub(c.workers.L[i].Last).Milliseconds() >
				(timeout - period).Milliseconds() {
				log.Printf("clean: %d", c.workers.L[i].Id)
				c.workers.L[i] = c.workers.L[len(c.workers.L)-1]
				c.workers.L = c.workers.L[:len(c.workers.L)-1]
			}
			if len(c.workers.L) == 0 {
				break
			}
		}
		log.Printf("clean: %s", c.workers.Str())
		c.workers.mu.Unlock()
		time.Sleep(period)
	}
}

func (c *Coordinator) reassign() {
	period, timeout := time.Duration(time.Second), time.Duration(10*time.Second)
	for {
		now := time.Now()
		c.tasks.mu.Lock()
		for i, t := range c.tasks.L {
			if now.Sub(t.Start).Milliseconds() >
				(timeout - period).Milliseconds() {
				c.tasks.L[i].Stat = TaskFree
			}
		}
		c.tasks.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) Heartb(arg *HbArg, rep *HbRep) error {
	c.workers.mu.Lock()
	defer c.workers.mu.Unlock()
	rep.Code = 0
	var w *Worker
	w = nil
	for i := 0; i < len(c.workers.L); i += 1 {
		if c.workers.L[i].Id == arg.Id {
			w = &c.workers.L[i]
		}
	}
	if w == nil {
		rep.Code = 1
	} else {
		w.Last = arg.Last
	}
	return nil
}

func (workers *Workers) maxId() int {
	max := -1
	for i := 0; i < len(workers.L); i += 1 {
		if workers.L[i].Id > max {
			max = workers.L[i].Id
		}
	}
	return max
}

func (c *Coordinator) RegW(arg *RegWArg, rep *RegWRep) error {
	c.workers.mu.Lock()
	defer c.workers.mu.Unlock()
	var newId int
	if len(c.workers.L) >= 1 {
		newId = c.workers.maxId() + 1
	} else {
		newId = 0
	}
	c.workers.L = append(c.workers.L, Worker{Id: newId, Last: time.Now()})
	rep.Id = newId
	rep.NRed = c.nRed
	rep.FCnt = c.fCnt
	log.Printf("RegW: %s, nRed: %d", c.workers.Str(), rep.NRed)
	return nil
}

func findT(ls []Task, num int, kind int) int {
	log.Printf("findT: num: %d, kind: %d", num, kind)
	for i, t := range ls {
		if t.Num == num && t.Type == kind {
			return i
		}
	}
	return -1
}

func last(ls []Task, kind int) int {
	for i := len(ls) - 1; i >= 0; i -= 1 {
		if ls[i].Type == kind {
			return i
		}
	}
	return -1
}

func (c *Coordinator) GetT(arg *GetTArg, rep *GetTRep) error {
	c.tasks.mu.Lock()
	defer c.tasks.mu.Unlock()
	i := findT(c.tasks.L, arg.DoneNum, arg.DoneType)
	if arg.DoneNum >= 0 && len(c.tasks.L) > 0 && i >= 0 {
		if c.tasks.L[i].Type == TaskR {
			c.muRedCnt.Lock()
			c.redCnt += 1
			c.muRedCnt.Unlock()
		}
		last := last(c.tasks.L, c.tasks.L[i].Type)
		c.tasks.L[i] = c.tasks.L[last]
		c.tasks.L[last] = c.tasks.L[len(c.tasks.L)-1]
		c.tasks.L = c.tasks.L[:len(c.tasks.L)-1]
	}
	log.Printf("GetT: tasks: %v", c.tasks.L)
	hasMap := false
	for _, t := range c.tasks.L {
		if t.Type == TaskM {
			hasMap = true
			break
		}
	}
	rep.Code = 1
	for i, t := range c.tasks.L {
		if t.Stat == TaskFree && !(t.Type == TaskR && hasMap) {
			rep.Code = 0
			rep.File = t.File
			rep.Type = t.Type
			rep.Num = t.Num
			c.tasks.L[i].Stat = TaskLive
			c.tasks.L[i].Start = time.Now()
			break
		}
	}
	if rep.Code == 1 && !c.Done() {
		rep.Code = 2
	}
	return nil
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{fCnt: len(files)}
	c.tasks = new(Tasks)
	c.tasks.fill(files, nReduce)
	c.workers = new(Workers)
	c.nRed = nReduce
	go c.clean()
	go c.reassign()
	c.server()
	return &c
}
