package mr

type taskType string

const (
	taskTypeMap    taskType = "map"
	taskTypeReduce taskType = "reduce"
)

type Location struct {
    // FilePath is relative to shared dir:
    // On Mac: mr-0-0.jsonl -> ~/Code/mnt/mr/mr-0-0.jsonl
    // On Deck: mr-0-0.jsonl -> /mnt/mr/mr-0-0.jsonl
	FilePath string
	FileSize int
}
type Task struct {
	Number    int
	Type      taskType
	FilePath  string     // empty for reduce task
	Locations []Location // nil for map task
}

type GetTaskArgs struct{}
type GetTaskReply struct {
	Task    *Task
	NReduce int
	Done    bool // indicates that there is no more work to be done. Worker can be terminated
}

type SubmitTaskArgs Task
type SubmitTaskReply struct{}
