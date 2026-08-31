package main

import (
	"encoding/json"
	"log"
	"os"
)

type Message struct {
	Key   string
	Value string
}

func main() {
	file, _ := os.Open("file.json")
	defer file.Close()
	dec := json.NewDecoder(file)

	var kv Message
	dec.Decode(&kv)
	
	log.Println(kv)
}
