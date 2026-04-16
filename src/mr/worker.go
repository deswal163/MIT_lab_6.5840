package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func processMapTask(askTaskReply *AskTaskReply, mapf func(string, string) []KeyValue) error {
	fileName := askTaskReply.TaskFile

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	file.Close()
	kva := mapf(fileName, string(content))
	var fileNames []*os.File
	var fileReaders []*json.Encoder
	for i := 0; i < askTaskReply.NReduce; i++ {
		// outFileName := fmt.Sprintf("mr-%v-%v", askTaskReply.TaskID, i)
		file, err = os.CreateTemp(".", fmt.Sprintf("mr-%v-%v-*", askTaskReply.TaskID, i))
		fileReaders = append(fileReaders, json.NewEncoder(file))
		fileNames = append(fileNames, file)
	}

	for _, kv := range kva {
		k := kv.Key
		index := ihash(k) % askTaskReply.NReduce
		oFile := fileReaders[index]
		oFile.Encode(&kv)
	}

	for i := 0; i < askTaskReply.NReduce; i++ {
		outFileName := fmt.Sprintf("mr-%v-%v", askTaskReply.TaskID, i)
		fileNames[i].Close()
		os.Rename(fileNames[i].Name(), outFileName)
	}

	return nil
}

func processReduceTask(args *AskTaskReply, reducef func(string, []string) string) error {
	// TaskID < mod NReduce
	filePathsPattern := fmt.Sprintf("mr-*-%v", args.TaskID)
	filePaths, err := filepath.Glob(filePathsPattern)

	if err != nil {
		fmt.Println("Failed to get files for reduce task :", err)
		return err
	}

	kva := []KeyValue{}

	for _, file := range filePaths {
		fileReader, err := os.Open(file)
		if err != nil {
			fmt.Println("Error reading the file :", file)
		}

		dec := json.NewDecoder(fileReader)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		fileReader.Close()
	}

	sort.Sort(ByKey(kva))

	outputFileName := fmt.Sprintf("mr-out-%v", args.TaskID)
	oFile, _ := os.CreateTemp(".", outputFileName)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	oFile.Close()
	os.Rename(oFile.Name(), outputFileName)

	return nil
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		askTaskArgs := new(AskTaskArgs)
		askTaskReply := new(AskTaskReply)
		success := call("Coordinator.AskTask", askTaskArgs, askTaskReply)
		if success {
			if askTaskReply.TaskAvailable == 0 {
				continue
			}
			fmt.Println("Got the task :", askTaskReply)

			switch askTaskReply.TaskType {
			case "map":
				if err := processMapTask(askTaskReply, mapf); err != nil {
					fmt.Println("Error :", err)
				}

				taskDoneArgs := TaskDoneArgs{
					TaskID:   askTaskReply.TaskID,
					Success:  true,
					TaskType: "map",
				}

				taskDoneReply := new(TaskDoneReply)

				if success := call("Coordinator.TaskDone", taskDoneArgs, taskDoneReply); !success {
					fmt.Println("Fail to update the Task!")
				}
			case "reduce":
				if err := processReduceTask(askTaskReply, reducef); err != nil {
					fmt.Println("Error :", err)
				}

				taskDoneArgs := TaskDoneArgs{
					TaskID:   askTaskReply.TaskID,
					Success:  true,
					TaskType: "reduce",
				}

				taskDoneReply := new(TaskDoneReply)

				if success := call("Coordinator.TaskDone", taskDoneArgs, taskDoneReply); !success {
					fmt.Println("Fail to update the Task!")
				}

			case "done":
				os.Exit(0)
			}

		} else {
			fmt.Println("Error getting the task !")
		}

	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
