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
	Num  int
}

type Tasks struct {
	mu   sync.Mutex
	List []Task
}

type Coordinator struct {
	workers  *Workers
	tasks    *Tasks
	nRed     int
	muRedCnt sync.Mutex
	redCnt   int
	fCnt     int
	kfMap    map[string]string
	muKf     sync.Mutex
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
	c.muRedCnt.Lock()
	defer c.muRedCnt.Unlock()
	return c.redCnt == c.nRed
}

func (tasks *Tasks) fill(files []string, nRed int) {
	for i := 0; i < len(files); i += 1 {
		log.Printf("fill: new task with file: %s", files[i])
		tasks.List = append(tasks.List,
			Task{File: files[i], Stat: TaskFree, Type: TaskM, Num: i})
	}
	for i := 0; i < nRed; i += 1 {
		tasks.List = append(tasks.List,
			Task{Stat: TaskFree, Type: TaskR,
				Num: i})
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
		// log.Printf("Heartb: %d", arg.Id)
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
	rep.FCnt = c.fCnt
	log.Printf("RegW: %s, nRed: %d", c.workers.Str(), rep.NRed)
	return nil
}

func maxNum(ls []Task, kind int) int {
	max := -1
	for _, t := range ls {
		if t.Type == kind && t.Num > max {
			max = t.Num
		}
	}
	return max
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
			log.Printf("last: %d", i)
			return i
		}
	}
	return -1
}

func (c *Coordinator) GetT(arg *GetTArg, rep *GetTRep) error {
	c.tasks.mu.Lock()
	defer c.tasks.mu.Unlock()
	tList := c.tasks.List
	if arg.DoneNum >= 0 && len(tList) > 0 {
		i := findT(tList, arg.DoneNum, arg.DoneType)
		if tList[i].Type == TaskR {
			c.muRedCnt.Lock()
			c.redCnt += 1
			c.muRedCnt.Unlock()
		} else {
			c.muKf.Lock()
			for k, f := range *arg.KfMap {
				c.kfMap[k] = f
			}
			c.muKf.Unlock()
		}
		last := last(tList, tList[i].Type)
		tList[i] = tList[last]
		tList[last] = tList[len(tList)-1]
		tList = tList[:len(tList)-1]
	}
	log.Printf("GetT: tasks: %v", tList)
	rep.Code = 1
	for i, t := range tList {
		if t.Stat == TaskFree {
			rep.Code = 0
			rep.File = t.File
			rep.Type = t.Type
			rep.Num = t.Num
			// log.Printf("GetT: rep: %v", rep)
			tList[i].Stat = TaskLive // keep track
			break
		}
	}
	c.tasks.List = tList
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
	c.kfMap = make(map[string]string)
	go c.clean()
	c.server()
	return &c
}
