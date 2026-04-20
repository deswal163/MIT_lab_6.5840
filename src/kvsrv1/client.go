package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}

	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{
			Key: key,
		}
		reply := rpc.GetReply{}

		if ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply); !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, reply.Err
		}

		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
	}
}

func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {

	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	reply := rpc.PutReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

	for !ok {
		time.Sleep(100 * time.Millisecond)
		reply = rpc.PutReply{}
		ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

		if ok && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
	}
	return reply.Err
}
