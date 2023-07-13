package main

import (
	"bufio"
	"mapreduce/mr"
	"strings"
)

var pattern string = "abc"

func Map(filename string, contents string) []mr.KeyValue {
	kva := new([]mr.KeyValue)
	reader := bufio.NewReader(strings.NewReader(contents))
	// reader.ReadString('\n')
	for {
		s, e := reader.ReadString('\n')
		if e != nil {
			break
		}
		if strings.Contains(s, pattern) {
			*kva = append(*kva, mr.KeyValue{Key: s})
		}
	}
	return *kva
}

func Reduce(key string, values []string) string {
	return key
}
