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

type MergedKey struct {
	Key  string
	Vals []string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func heartb(id int) {
	for {
		arg, rep := &HbArg{Id: id, Last: time.Now()}, HbRep{}
		e := call("Coordinator.Heartb", arg, &rep)
		if !e {
			Fail("heartb: call", nil)
		}
		if rep.Code == 1 {
			break
		}
		log.Printf("heartb: sent")
		time.Sleep(time.Second)
	}
}

func reg() (int, int) {
	arg, rep := &RegWArg{}, RegWRep{}
	e := call("Coordinator.RegW", arg, &rep)
	if !e {
		Fail("reg: call", nil)
	}
	log.Printf("reg: id: %d, nRed: %d", rep.Id, rep.NRed)
	return rep.Id, rep.NRed
}

func combine(kva []KeyValue) *[]MergedKey {
	keys := new([]MergedKey)
	for i := 0; i < len(kva); i += 1 {
		key := kva[i].Key
		j := i
		var vals []string
		for ; j < len(kva) && kva[j].Key == key; j += 1 {
			vals = append(vals, kva[j].Value)
		}
		i = j + 1
		*keys = append(*keys, MergedKey{Key: key, Vals: vals})
	}
	return keys
}

func bucket(keys *[]MergedKey, nRed int) *[][]MergedKey {
	bucks := new([][]MergedKey)
	size := len(*keys)
	wid, rem := size/nRed, size%nRed
	for i := 0; i < nRed; i += 1 {
		*bucks = append(*bucks, (*keys)[i*wid:(i+1)*wid])
	}
	for i := size - rem; i < size; i += 1 {
		bNum := nRed - rem + i - size
		(*bucks)[bNum] = append((*bucks)[bNum], (*keys)[i])
	}
	return bucks
}

func doMap(f string, mapf func(string, string) []KeyValue, nRed int, tNum int) {
	bytes, e := os.ReadFile(f)
	if e != nil {
		Fail("doTask: os.ReadFile", e)
	}
	kva := mapf("", string(bytes[:]))
	sort.Sort(ByKey(kva))
	bucks := bucket(combine(kva), nRed)
	for i := 0; i < nRed; i += 1 {
		f, e := os.Create(fmt.Sprintf("mr-%d-%d", tNum, i))
		if e != nil {
			Fail("write: os.Create", e)
		}
		enc := json.NewEncoder(f)
		enc.Encode((*bucks)[i])
	}
}

func read(keys *[]MergedKey, fName string) {
	f, e := os.Open(fName)
	if e != nil {
		Fail("read: os.Open", e)
	}
	dec := json.NewDecoder(f)
	key := new(MergedKey)
	for {
		e := dec.Decode(key)
		if e != nil {
			Fail("read: dec.Decode", e)
		}
		*keys = append(*keys, *key)
	}
}

func doRed(redf func(string, []string) string, fName string, tNum int) {
	keys := new([]MergedKey)
	read(keys, fName)
	f, e := os.Create(fmt.Sprintf("mr-out-%d", tNum))
	if e != nil {
		Fail("doRead: os.Create", e)
	}
	for _, key := range *keys {
		fmt.Fprintf(f, "%v %v\n", key.Key, redf(key.Key, key.Vals))
	}
}

func doTask(id int, nRed int, mapf func(string, string) []KeyValue,
	redf func(string, []string) string) {
	for {
		arg, rep := &GetTArg{Id: id}, GetTRep{}
		e := call("Coordinator.GetT", arg, &rep)
		if !e {
			Fail("doTask: call", nil)
		}
		if rep.Code == 1 {
			break
		}
		if rep.Type == TaskM {
			doMap(rep.File, mapf, nRed, rep.Num)
		} else {
			doRed(redf, rep.File, rep.Num)
		}
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
	id, nRed := reg()
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
		doTask(id, nRed, mapf, redf)
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
