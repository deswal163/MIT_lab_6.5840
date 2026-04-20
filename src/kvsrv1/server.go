package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type keyValue struct {
	key     string
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	kvStore map[string]*keyValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvStore = make(map[string]*keyValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exist := kv.kvStore[args.Key]

	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = value.value
	reply.Version = value.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, exist := kv.kvStore[args.Key]

	if !exist {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		record = &keyValue{
			key: args.Key,
		}
	}

	if args.Version != record.version {
		reply.Err = rpc.ErrVersion
		return
	}

	record.value = args.Value
	record.version += 1
	kv.kvStore[args.Key] = record
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
