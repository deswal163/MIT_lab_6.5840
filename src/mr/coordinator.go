package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var nRD int

type Task struct {
	TaskID        int
	TaskState     string
	TaskFile      string
	taskStartTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	MapTasks    []*Task
	ReduceTasks []*Task
	phase       string
}

func checkAllTaskDone(tasks []*Task) bool {
	for i := range tasks {
		if tasks[i].TaskState != "Done" {
			return false
		}
	}
	return true
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case "Map":
		for index, task := range c.MapTasks {
			if task.TaskState == "Idle" {
				reply.NReduce = nRD
				reply.TaskAvailable = 1
				reply.TaskID = index
				reply.TaskFile = task.TaskFile
				reply.TaskType = "map"
				task.TaskState = "InProgress"
				task.taskStartTime = time.Now()
				return nil
			}
		}
	case "Reduce":
		for index, task := range c.ReduceTasks {
			if task.TaskState == "Idle" {
				reply.NReduce = nRD
				reply.TaskAvailable = 1
				reply.TaskID = index
				reply.TaskType = "reduce"
				task.TaskState = "InProgress"
				task.taskStartTime = time.Now()
				return nil
			}
		}
	case "Done":
		reply.TaskType = "done"
		reply.TaskAvailable = 1
		return nil
	}

	// create ReduceTasks
	return nil
}

func processMapTaskDone(c *Coordinator, args *TaskDoneArgs, _ *TaskDoneReply) error {
	task := c.MapTasks[args.TaskID]

	if task.TaskState != "Done" {
		task.TaskState = "Done"
	}

	return nil
}

func processReduceTaskDone(c *Coordinator, args *TaskDoneArgs, _ *TaskDoneReply) error {

	task := c.ReduceTasks[args.TaskID]

	if task.TaskState != "Done" {
		task.TaskState = "Done"
	}

	return nil
}

func createReduceTasks(c *Coordinator) {
	for i := range nRD {
		c.ReduceTasks = append(c.ReduceTasks, &Task{
			TaskID:    i,
			TaskState: "Idle",
		})
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case "map":
		if err := processMapTaskDone(c, args, reply); err != nil {
			fmt.Println("Proccesing Map task failed :", args)
		}

		if checkAllTaskDone(c.MapTasks) && c.phase == "Map" {
			c.phase = "Reduce"
			createReduceTasks(c)
		}
	case "reduce":
		if err := processReduceTaskDone(c, args, reply); err != nil {
			fmt.Println("Proccessing Reduce Task failed :", args)
		}

		if checkAllTaskDone(c.ReduceTasks) && c.phase == "Reduce" {
			c.phase = "Done"
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase != "Done" {
		return false
	}
	return true
}

func createTask(c *Coordinator, files []string) {
	for index, file := range files {
		task := &Task{
			TaskID:    index,
			TaskState: "Idle",
			TaskFile:  file,
		}
		c.MapTasks = append(c.MapTasks, task)
	}
}

func checkForFailure(task *Task) bool {
	if time.Now().Add(-10 * time.Second).After(task.taskStartTime) {
		return true
	}
	return false
}

func checkFailureTasks(tasks []*Task) {
	for i := range tasks {
		if checkForFailure(tasks[i]) {
			tasks[i].TaskState = "Idle"
		}
	}
}

func checkForTimeOuts(c *Coordinator) error {

	for {
		time.Sleep(2 * time.Second)

		c.mu.Lock()
		checkFailureTasks(c.MapTasks)
		checkFailureTasks(c.ReduceTasks)
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase: "Map",
	}

	nRD = nReduce
	createTask(&c, files)
	go checkForTimeOuts(&c)
	c.server(sockname)
	return &c
}
