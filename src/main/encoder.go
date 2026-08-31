package main

import (
	"encoding/json"
	"os"
)

type Message struct {
	Key   string
	Value string
}

func main() {
	file, _ := os.Create("file.json")
	defer file.Close()
	enc := json.NewEncoder(file)

	enc.Encode(Message{"a", "1"})
	enc.Encode(Message{"a", "1"})

}
