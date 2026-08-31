package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type taskState string

const taskTimeout = 10 * time.Second
const (
	taskStateIdle       taskState = "idle"
	taskStateInProgress taskState = "in-progress"
	taskStateCompleted  taskState = "completed"
)

type location struct {
	filePath string
	fileSize int
}
type task struct {
	taskType  taskType
	number    int
	filePath  string
	state     taskState
	locations []location
}

type Coordinator struct {
	mu      sync.Mutex
	tasks   []task
	nReduce int
	done    bool
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// maps are shared, so read and writes should be synchronized
	c.mu.Lock()
	defer c.mu.Unlock()
	// search for idle task
	i := slices.IndexFunc(c.tasks, func(t task) bool {
		return (t.taskType == taskTypeMap && t.state == taskStateIdle) ||
			// start reduce dispatching only when all maps are done
			(t.taskType == taskTypeReduce && (c.mapsDone()) && t.state == taskStateIdle)
	})
	if i == -1 {
		log.Println("no idle task to dispatch")
		return nil
	}
	task := &c.tasks[i]
	task.state = taskStateInProgress

	var locations []Location
	if task.taskType == taskTypeReduce {
		for _, t := range c.tasks {
			if t.taskType == taskTypeReduce {
				continue
			}
			i := slices.IndexFunc(t.locations, func(l location) bool {
				suffix := fmt.Sprintf("-%v.jsonl", task.number) // mr-X-Y.jsonl, where Y is a reduce task number
				return strings.HasSuffix(l.filePath, suffix)
			})
			if i != -1 {
				location := t.locations[i]
				locations = append(locations, Location{location.filePath, location.fileSize})
			}
		}
	}
	reply.Task = &Task{Number: task.number, Type: task.taskType, FilePath: task.filePath, Locations: locations}
	reply.NReduce = c.nReduce
	reply.Done = c.done

	// Transition a task from in-progress to idle state in case it won't be completed within
	// the task timeout
	time.AfterFunc(taskTimeout, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		if task.state == taskStateCompleted {
			return
		}

		task.state = taskStateIdle
		log.Println("reset long-running task", *task)
	})

	log.Println("dispatched task", *task)

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	submitted := args

	c.mu.Lock()
	defer c.mu.Unlock()

	i := slices.IndexFunc(c.tasks, func(t task) bool {
		return t.taskType == submitted.Type && t.number == submitted.Number
	})
	if i == -1 {
		log.Printf("task to submit not found: type=%v number=%v\n", submitted.Type, submitted.Number)
		return errors.New("task to submit not found")
	}

	locations := make([]location, len(submitted.Locations))
	for i, l := range submitted.Locations {
		locations[i] = location{l.FilePath, l.FileSize}
	}

	target := &c.tasks[i]
	target.locations = locations
	target.state = taskStateCompleted

	log.Printf("submitted task: type=%v number=%v locations=%v\n", submitted.Type, submitted.Number, submitted.Locations)

	allCompleted := true
	for _, t := range c.tasks {
		if t.state != taskStateCompleted {
			allCompleted = false
			break
		}
	}
	c.done = allCompleted

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockOrAddr string) {
	rpc.Register(c)
	rpc.HandleHTTP()

	network := "tcp"
	if os.Getenv("ENVIRONMENT") == "test" {
		network = "unix"
		if err := os.Remove(sockOrAddr); err != nil && !os.IsNotExist(err) {
			log.Fatalf("remove socket %s: %v", sockOrAddr, err)
		}
	}

	l, err := net.Listen(network, sockOrAddr)
	if err != nil {
		log.Fatalf("listen error %s: %v", sockOrAddr, err)
	}

	go http.Serve(l, nil)
	log.Printf("server listening on %s: %s", network, sockOrAddr)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) mapsDone() bool {
	done := true
	for _, t := range c.tasks {
		if t.taskType == taskTypeMap && t.state != taskStateCompleted {
			done = false
		}
	}
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockOrAddr string, files []string, nReduce int) *Coordinator {
	maps := make([]task, len(files))
	for i, f := range files {
		maps[i] = task{taskTypeMap, i, f, taskStateIdle, nil}
	}
	reduces := make([]task, nReduce)
	for i := range nReduce {
		reduces[i] = task{taskTypeReduce, i, "", taskStateIdle, nil}
	}
	tasks := append(maps, reduces...)
	c := Coordinator{sync.Mutex{}, tasks, nReduce, false}

	// Your code here.

	c.server(sockOrAddr)
	return &c
}
