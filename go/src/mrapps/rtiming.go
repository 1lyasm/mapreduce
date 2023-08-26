package main

import (
	"fmt"
	"io/ioutil"
	"mapreduce/mr"
	"os"
	"syscall"
	"time"
)

func nparallel(phase string) int {
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			err := syscall.Kill(xpid, 0)
			if err == nil {
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", "1"})
	kva = append(kva, mr.KeyValue{"b", "1"})
	kva = append(kva, mr.KeyValue{"c", "1"})
	kva = append(kva, mr.KeyValue{"d", "1"})
	kva = append(kva, mr.KeyValue{"e", "1"})
	kva = append(kva, mr.KeyValue{"f", "1"})
	kva = append(kva, mr.KeyValue{"g", "1"})
	kva = append(kva, mr.KeyValue{"h", "1"})
	kva = append(kva, mr.KeyValue{"i", "1"})
	kva = append(kva, mr.KeyValue{"j", "1"})
	return kva
}

func Reduce(key string, values []string) string {
	n := nparallel("reduce")

	val := fmt.Sprintf("%d", n)

	return val
}
