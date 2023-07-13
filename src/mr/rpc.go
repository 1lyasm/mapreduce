package mr

import (
	"log"
	"os"
	"strconv"
	"time"
)

type RegWArg struct{}

type RegWRep struct {
	Id   int
	NRed int
	FCnt int
}

type HbArg struct {
	Id   int
	Last time.Time
}

type HbRep struct {
	Code int
}

type GetTArg struct {
	DoneNum  int
	DoneType int
	KfMap    *map[string]string
}

type GetTRep struct {
	Code int
	File string
	Type int
	Num  int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func Fail(f string, e error) {
	log.Fatal(f + " failed: " + e.Error())
}
