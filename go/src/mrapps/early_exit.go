package main

import (
	"strconv"
	"strings"
	"time"

	"mapreduce/mr"
)

func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{filename, "1"})
	return kva
}

func Reduce(key string, values []string) string {
	if strings.Contains(key, "sherlock") || strings.Contains(key, "tom") {
		time.Sleep(time.Duration(3 * time.Second))
	}
	return strconv.Itoa(len(values))
}
