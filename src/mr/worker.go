package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	mapTempPattern     = "mr-%v-%v-*.jsonl"
	mapFinalPattern    = "mr-%v-%v.jsonl"
	reduceTempPattern  = "mr-out-%v-*"
	reduceFinalPattern = "mr-out-%v"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

// main/mrworker.go calls this function.
func Worker(sockname string, m func(string, string) []KeyValue,
	r func(string, []string) string) {
	coordSockName = sockname
	mapf = m
	reducef = r

	// Your worker implementation here.

	finished := ExecuteTask()
	if finished {
		log.Println("execution completed")
		os.Exit(0)
	}
}

func ExecuteTask() (finished bool) {
	reply := CallGetTask()
	if reply.Done {
		return reply.Done
	}
	log.Println("got a task", *reply.Task)

	switch reply.Task.Type {
	case taskTypeMap:
		ExecuteMap(reply)
	case taskTypeReduce:
		ExecuteReduce(reply)
	default:
		log.Fatalln("unsupported task type:", reply.Task.Type)
	}

	return ExecuteTask()
}

func ExecuteMap(reply GetTaskReply) {
	task := reply.Task
	filePath := task.FilePath
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("cannot read input file %v", filePath)
	}
	intermediate := mapf(filePath, string(content))

	dir := os.Getenv("SHARED_DIR")
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			log.Fatalln("failed to get working directory:", err)
		}
		dir = wd
	}
	files := make([]*os.File, reply.NReduce)
	writers := make([]*bufio.Writer, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := range reply.NReduce {
		file, err := os.CreateTemp(dir, fmt.Sprintf(mapTempPattern, task.Number, i))
		if err != nil {
			log.Fatalln("failed to create intermediate file:", err)
		}
		files[i] = file
		writers[i] = bufio.NewWriterSize(file, 64*1024) // use buffered writes to reduce system calls
		encoders[i] = json.NewEncoder(writers[i])
	}

	for _, kv := range intermediate {
		reduce := ihash(kv.Key) % reply.NReduce

		if err := encoders[reduce].Encode(&kv); err != nil {
			log.Fatalln("failed to encode intermediate record:", err)
		}
	}

	locations := make([]Location, reply.NReduce)
	for i, f := range files {
		if err := writers[i].Flush(); err != nil {
			log.Fatalln(err)
		}

		info, err := f.Stat()
		if err != nil {
			log.Fatalln("failed to stat intermediate file:", err)
		}
		if err = f.Close(); err != nil {
			log.Fatalln("failed to close intermediate file:", err)
		}

		tempPath := f.Name()
		finalPath := filepath.Join(dir, fmt.Sprintf(mapFinalPattern, task.Number, i))
		if err := os.Rename(tempPath, finalPath); err != nil {
			log.Fatalln("failed to rename intermediate file:", err)
		}

		locations[i] = Location{filepath.Base(finalPath), int(info.Size())}
	}
	task.Locations = locations

	CallSubmitTask(*task)
	log.Printf("task was submitted: type=%v number=%v locations=%v\n", task.Type, task.Number, task.Locations)
}

func ExecuteReduce(reply GetTaskReply) {
	task := reply.Task

	dir := os.Getenv("SHARED_DIR")
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			log.Fatalln("failed to get working directory:", err)
		}
		dir = wd
	}

	var intermediate []KeyValue
	for _, l := range task.Locations {
		path := filepath.Join(dir, l.FilePath)
		file, err := os.Open(path)
		if err != nil {
			log.Fatalln("failed to open file:", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln("failed to decode json:", err)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	file, err := os.CreateTemp(dir, fmt.Sprintf(reduceTempPattern, task.Number))
	writer := bufio.NewWriterSize(file, 64*1024)
	if err != nil {
		log.Fatalln("failed to create temp output file:", err)
	}
	tempPath := file.Name()
	finalPath := filepath.Join(dir, fmt.Sprintf(reduceFinalPattern, task.Number))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	if err := writer.Flush(); err != nil {
		log.Fatalln(err)
	}
	file.Close()
	log.Println("wrote temp output file:", tempPath)

	if err := os.Rename(tempPath, finalPath); err != nil {
		log.Fatalln("failed to rename output file:", err)
	}

	CallSubmitTask(*task)
	log.Printf("task was submitted: type=%v number=%v locations=%v\n", task.Type, task.Number, task.Locations)
}

// CallGetTask calls Coordinator.GetTask method in a loop until it gets
// a task or the coordinator says that there are no more tasks to execute
func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)

	for reply.Task == nil && !reply.Done {
		log.Println("got no task, sleep for 1 second before next call")
		time.Sleep(1 * time.Second)

		reply = GetTaskReply{}
		call("Coordinator.GetTask", &args, &reply)
	}

	return reply
}

func CallSubmitTask(task Task) SubmitTaskReply {
	args := task
	reply := SubmitTaskReply{}
	call("Coordinator.SubmitTask", &args, &reply)
	return reply
}

// call sends an RPC request to the coordinator, wait for the response.
// Panics in case of an error
func call(rpcname string, args interface{}, reply interface{}) error {
	network := "tcp"
	addr := os.Getenv("COORD_ADDR")

	if os.Getenv("ENVIRONMENT") == "test" {
		network = "unix"
		addr = coordSockName
	} else if addr == "" {
		addr = "127.0.0.1:8000"
	}

	c, err := rpc.DialHTTP(network, addr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	log.Println("try calling", rpcname)
	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Println("error calling", rpcname)
		log.Println(err)
	}
	return err
}
