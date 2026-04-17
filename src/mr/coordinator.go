// Package mr implements a simple MapReduce coordinator.
//
// Coordinator state (phase, map/reduce tasks) is owned by a single goroutine
// (coordinatorLoop). RPC handlers do not touch that state directly; they send
// requests on channels and block on reply channels. Timeouts are handled in the
// same loop via a ticker—one place to read for the job lifecycle.
package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	TaskID        int
	TaskState     string
	TaskFile      string
	taskStartTime time.Time
}

// askTaskReq is a request from an RPC handler to the state loop: please fill reply.
type askTaskReq struct {
	reply chan<- AskTaskReply
}

// taskDoneReq reports completion of a task to the state loop.
type taskDoneReq struct {
	args  TaskDoneArgs
	reply chan<- struct{}
}

// doneQueryReq asks whether the whole job is finished (mrcoordinator polls this).
type doneQueryReq struct {
	reply chan<- bool
}

type Coordinator struct {
	nReduce int

	MapTasks    []*Task
	ReduceTasks []*Task
	phase       string

	askCh       chan askTaskReq
	taskDoneCh  chan taskDoneReq
	doneQueryCh chan doneQueryReq
}

func checkAllTaskDone(tasks []*Task) bool {
	for i := range tasks {
		if tasks[i].TaskState != "Done" {
			return false
		}
	}
	return true
}

func (c *Coordinator) AskTask(_ *AskTaskArgs, reply *AskTaskReply) error {
	replyCh := make(chan AskTaskReply, 1)
	c.askCh <- askTaskReq{reply: replyCh}
	*reply = <-replyCh
	return nil
}

func (c *Coordinator) handleAskTask() AskTaskReply {
	var reply AskTaskReply
	switch c.phase {
	case "Map":
		for index, task := range c.MapTasks {
			if task.TaskState == "Idle" {
				reply.NReduce = c.nReduce
				reply.TaskAvailable = 1
				reply.TaskID = index
				reply.TaskFile = task.TaskFile
				reply.TaskType = "map"
				task.TaskState = "InProgress"
				task.taskStartTime = time.Now()
				return reply
			}
		}
	case "Reduce":
		for index, task := range c.ReduceTasks {
			if task.TaskState == "Idle" {
				reply.NReduce = c.nReduce
				reply.TaskAvailable = 1
				reply.TaskID = index
				reply.TaskType = "reduce"
				task.TaskState = "InProgress"
				task.taskStartTime = time.Now()
				return reply
			}
		}
	case "Done":
		reply.TaskType = "done"
		reply.TaskAvailable = 1
		return reply
	}
	return reply
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
	for i := 0; i < c.nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &Task{
			TaskID:    i,
			TaskState: "Idle",
		})
	}
}

func (c *Coordinator) handleTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) {
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
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	done := make(chan struct{}, 1)
	c.taskDoneCh <- taskDoneReq{args: *args, reply: done}
	<-done
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
	replyCh := make(chan bool, 1)
	c.doneQueryCh <- doneQueryReq{reply: replyCh}
	return <-replyCh
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

// coordinatorLoop is the only goroutine that mutates phase and task slices.
func (c *Coordinator) coordinatorLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var reply TaskDoneReply
	for {
		select {
		case req := <-c.askCh:
			req.reply <- c.handleAskTask()

		case req := <-c.taskDoneCh:
			c.handleTaskDone(&req.args, &reply)
			req.reply <- struct{}{}

		case req := <-c.doneQueryCh:
			req.reply <- (c.phase == "Done")

		case <-ticker.C:
			checkFailureTasks(c.MapTasks)
			checkFailureTasks(c.ReduceTasks)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:       "Map",
		nReduce:     nReduce,
		askCh:       make(chan askTaskReq),
		taskDoneCh:  make(chan taskDoneReq),
		doneQueryCh: make(chan doneQueryReq),
	}

	createTask(c, files)
	go c.coordinatorLoop()
	c.server(sockname)
	return c
}
