package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AskTaskArgs struct {
}

type AskTaskReply struct {
	TaskAvailable int
	TaskType      string
	TaskID        int
	TaskFile      string
	NReduce       int
}

type TaskDoneArgs struct {
	TaskID   int
	Success  bool
	TaskType string
}

type TaskDoneReply struct {
}
