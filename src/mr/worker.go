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

func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
		bNum := nRed - rem + (i - size + rem)
		(*bucks)[bNum] = append((*bucks)[bNum], (*keys)[i])
	}
	return bucks
}

type FileKeys struct {
	F    string
	Keys []string
}

func doMap(f string, mapf func(string, string) []KeyValue, nRed int, tNum int) *[]FileKeys {
	log.Printf("doMap: f: %s", f)
	bytes, e := os.ReadFile(f)
	if e != nil {
		Fail("doMap: os.ReadFile", e)
	}
	kva := mapf("", string(bytes[:]))
	sort.Sort(ByKey(kva))
	bucks := bucket(combine(kva), nRed)
	fkList := new([]FileKeys)
	for i := 0; i < nRed; i += 1 {
		fName := fmt.Sprintf("mr-%d-%d", tNum, i)
		f, e := os.Create(fName)
		if e != nil {
			Fail("write: os.Create", e)
		}
		enc := json.NewEncoder(f)
		enc.Encode((*bucks)[i])
		keys := *new([]string)
		for _, mkey := range (*bucks)[i] {
			keys = append(keys, mkey.Key)
		}
		*fkList = append(*fkList, FileKeys{F: fName, Keys: keys})
	}
	return fkList
}

func read(keys *[]MergedKey, fName string) {
	f, e := os.Open(fName)
	if e != nil {
		Fail("read: os.Open", e)
	}
	dec := json.NewDecoder(f)
	dec.Decode(keys)
}

func gather(vals *[]string, targetK MergedKey, kLocs map[string][]string) {
	// log.Printf("gather: kLocs: %v", kLocs)
	for _, fName := range kLocs[targetK.Key] {
		keys := new([]MergedKey)
		read(keys, fName)
		for _, mkey := range *keys {
			if mkey.Key == targetK.Key {
				// log.Printf("gather: appending")
				*vals = append(*vals, mkey.Vals...)
			}
		}
	}
}

func doRed(redf func(string, []string) string, redNum int,
	fCnt int, nRed int, kLocs map[string][]string) {
	f, e := os.OpenFile(fmt.Sprintf("mr-out-%d", redNum), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		Fail("doRed: os.Create", e)
	}
	for i := 0; i < fCnt; i += 1 {
		keys := new([]MergedKey)
		read(keys, fmt.Sprintf("mr-%d-%d", i, redNum))
		for _, key := range *keys {
			// log.Printf("doRed: main key.Key: %s", key.Key)
			key.Vals = *new([]string)
			gather(&key.Vals, key, kLocs)
			fmt.Fprintf(f, "%v %v\n", key.Key, redf(key.Key, key.Vals))
		}
	}
}

func doTask(id int, nRed int, mapf func(string, string) []KeyValue,
	redf func(string, []string) string, fCnt int) {
	doneNum := -1
	doneType := -1
	var fkList *[]FileKeys
	for {
		arg, rep := &GetTArg{DoneNum: doneNum, DoneType: doneType,
			FkList: fkList}, GetTRep{}
		_, e := call("Coordinator.GetT", arg, &rep)
		if e != nil {
			Fail("doTask: call", e)
		}
		if rep.Code == 1 {
			break
		} else if rep.Code == 2 {
			log.Printf("doTask: waiting for new tasks")
			doneNum = -1
			time.Sleep(time.Second)
			continue
		}
		if rep.Type == TaskM {
			fkList = doMap(rep.File, mapf, nRed, rep.Num)
		} else {
			doRed(redf, rep.Num, fCnt, nRed, rep.KeyLocs)
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
