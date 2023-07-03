package common

import (
	"log"
	"time"
)

const (
	HostIp string = "192.168.1.12"
	Port          = "1234"
)

func Fail(funcName string, e error) {
	log.Fatal(funcName + " failed: " + e.Error())
}

type RegWorkerArg struct {
}

type RegWorkerReply struct {
	Id int
}

type UpdateLastSeenArg struct {
	Id       int
	LastSeen time.Time
}

type UpdateLastSeenReply struct {
}
