package main

import (
	"mapreduce/mr"
	"strings"
)

var pattern string = "lish"

func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	for _, line := range strings.Split(strings.TrimSuffix(contents, "\n"), "\n") {
		if strings.Contains(line, pattern) {
			kva = append(kva, mr.KeyValue{Key: line, Value: filename})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	res := ""
	for i, line := range values {
		if i != len(values)-1 {
			res += line + ","
		} else {
			res += line
		}
	}
	return res
}
