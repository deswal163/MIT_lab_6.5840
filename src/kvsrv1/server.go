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

// keyValue is the per-key state stored in the server. The key itself is
// implicit in the map index and therefore not repeated here.
type keyValue struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]keyValue
}

func MakeKVServer() *KVServer {
	return &KVServer{
		store: make(map[string]keyValue),
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.store[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK
}

// Put installs args.Value for args.Key if args.Version matches the
// server's current version for that key, and increments the stored
// version on success. If the key doesn't exist, Put installs the value
// only when args.Version is 0; otherwise it returns ErrNoKey. A version
// mismatch returns ErrVersion.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, exists := kv.store[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
	} else if args.Version != entry.version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.store[args.Key] = keyValue{
		value:   args.Value,
		version: entry.version + 1,
	}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
