package main

import (
	"fmt"
	"mapreduce/mr"
	"sort"
	"strings"
	"unicode"
)

func Map(document string, value string) (res []mr.KeyValue) {
	m := make(map[string]bool)
	words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
	for _, w := range words {
		m[w] = true
	}
	for w := range m {
		kv := mr.KeyValue{w, document}
		res = append(res, kv)
	}
	return
}

func Reduce(key string, values []string) string {
	sort.Strings(values)
	return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
}
