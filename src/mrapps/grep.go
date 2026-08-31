package main

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin grep.go
//

import (
	"fmt"
	"regexp"
	"strings"

	"6.5840/mr"
)

// pattern is the hardcoded regular expression to search for.
var pattern = regexp.MustCompile(`(?i)\bthe\b`)

// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. It scans the contents line by line and,
// for each line matching the pattern, emits a key/value pair where
// the key is "path-to-file:line-number" and the value is the line.
func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}

	lines := strings.Split(contents, "\n")
	for i, line := range lines {
		lineno := i + 1
		if pattern.MatchString(line) {
			kv := mr.KeyValue{fmt.Sprintf("%s:%d", filename, lineno), line}
			kva = append(kva, kv)
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strings.Join(values, "\n")
}
