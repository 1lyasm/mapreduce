package common

import "log"

const (
	IpAddr string = "192.168.1.7"
	Port   string = "1234"
)

func Fail(funcName string, e error) {
	log.Fatal(funcName + " failed: " + e.Error())
}

type RegWorkerArg struct {
}

type RegWorkerReply struct {
}
