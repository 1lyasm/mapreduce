package main

import (
	"fmt"
	"io"
	"log"
	"mapreduce/mr"
	"os"
	"time"
)

func main() {
	log.SetOutput(io.Discard)
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
