package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}
	fmt.Println(err)
	return false, err
}

func heartb(id int) {
	for {
		arg, rep := &HbArg{Id: id, Last: time.Now()}, HbRep{}
		_, e := call("Coordinator.Heartb", arg, &rep)
		if e != nil {
			Fail("heartb: call", e)
		}
		if rep.Code == 1 {
			break
		}
		log.Printf("heartb: sent")
		time.Sleep(time.Second)
	}
}

func reg() (int, int, int) {
	arg, rep := &RegWArg{}, RegWRep{}
	_, e := call("Coordinator.RegW", arg, &rep)
	if e != nil {
		Fail("reg: call", e)
	}
	log.Printf("reg: id: %d, nRed: %d, FCnt: %d", rep.Id, rep.NRed, rep.FCnt)
	return rep.Id, rep.NRed, rep.FCnt
}

func combine(kva []KeyValue) *map[string][]string {
	combined := make(map[string][]string)
	for i := 0; i < len(kva); i += 1 {
		combined[kva[i].Key] = append(combined[kva[i].Key], kva[i].Value)
	}
	return &combined
}

func doMap(f string, mapf func(string, string) []KeyValue,
	nRed int, tNum int) {
	log.Printf("doMap: f: %s", f)
	bytes, e := os.ReadFile(f)
	if e != nil {
		Fail("doMap: os.ReadFile", e)
	}
	kva := mapf(f, string(bytes[:]))
	for i := 0; i < nRed; i += 1 {
		_, e := os.Create(fmt.Sprintf("mr-%d-%d", tNum, i))
		if e != nil {
			Fail("doMap: os.Create", e)
		}
	}
	kvfMap := make(map[string][]KeyValue)
	for _, kv := range kva {
		redW := ihash(kv.Key) % nRed
		intF := fmt.Sprintf("mr-%d-%d", tNum, redW)
		kvfMap[intF] = append(kvfMap[intF], kv)
	}
	for fName, kva := range kvfMap {
		intF, e := os.Create(fName)
		if e != nil {
			Fail("doMap: os.Open", e)
		}
		enc := json.NewEncoder(intF)
		e = enc.Encode(kva)
		if e != nil {
			Fail("doMap: enc.Encode", e)
		}
	}
}

func read(kva *[]KeyValue, fName string) {
	f, e := os.Open(fName)
	if e != nil {
		Fail("read: os.Open", e)
	}
	dec := json.NewDecoder(f)
	dec.Decode(kva)
}

func doRed(redf func(string, []string) string, redNum int,
	fCnt int, nRed int) {
	log.Printf("doRed: redNum: %d", redNum)
	f, e := os.Create(fmt.Sprintf("mr-out-%d", redNum))
	if e != nil {
		Fail("doRed: os.Create", e)
	}
	kvaAll := new([]KeyValue)
	for i := 0; i < fCnt; i += 1 {
		kva := new([]KeyValue)
		read(kva, fmt.Sprintf("mr-%d-%d", i, redNum))
		*kvaAll = append(*kvaAll, *kva...)
	}
	sort.Sort(ByKey(*kvaAll))
	combined := combine(*kvaAll)
	for key, vals := range *combined {
		res := redf(key, vals)
		fmt.Fprintf(f, "%v %v\n", key, res)
	}
}

func doTask(id int, nRed int, mapf func(string, string) []KeyValue,
	redf func(string, []string) string, fCnt int) {
	doneNum := -1
	doneType := -1
	for {
		arg, rep := &GetTArg{DoneNum: doneNum, DoneType: doneType}, GetTRep{}
		_, e := call("Coordinator.GetT", arg, &rep)
		if e != nil {
			Fail("doTask: call", e)
		}
		if rep.Code == 1 {
			break
		} else if rep.Code == 2 {
			log.Printf("doTask: w%d: waiting for new tasks", id)
			doneNum = -1
			time.Sleep(time.Second)
			continue
		}
		if rep.Type == TaskM {
			doMap(rep.File, mapf, nRed, rep.Num)
		} else {
			doRed(redf, rep.Num, fCnt, nRed)
		}
		doneNum = rep.Num
		doneType = rep.Type
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
	id, nRed, fCnt := reg()
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
		doTask(id, nRed, mapf, redf, fCnt)
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
