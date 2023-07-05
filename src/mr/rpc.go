package common

import (
	"log"
	"time"
)

const (
	HostIp = "192.168.1.106"
	Port   = "1234"
)

func Fail(funcName string, e error) {
	log.Fatal(funcName + " failed: " + e.Error())
}

type RegWorkerArg struct {
}

type RegWorkerRep struct {
	Id int
}

type UpdateLastSeenArg struct {
	Id       int
	LastSeen time.Time
}

type UpdateLastSeenRep struct {
	Code int
}

type GetTaskArg struct {
	Id int
}

type GetTaskRep struct {
	Code int
	File string
}
