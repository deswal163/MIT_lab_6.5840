package kvsrv

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// retryBackoff is how long the Clerk waits before retrying an RPC that
// did not receive a reply. Kept short so unreliable-network tests make
// progress without saturating the scheduler.
const retryBackoff = 100 * time.Millisecond

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	return &Clerk{clnt: clnt, server: server}
}

// Get retries until it receives a reply. It returns either (value,
// version, OK) or ("", 0, ErrNoKey).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		reply := rpc.GetReply{}
		if !ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) {
			time.Sleep(retryBackoff)
			continue
		}
		switch reply.Err {
		case rpc.OK:
			return reply.Value, reply.Version, rpc.OK
		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey
		default:
			log.Fatalf("kvsrv Clerk.Get: unexpected reply.Err %q", reply.Err)
		}
	}
}

// Put retries until it receives a reply. The tricky case is
// ErrVersion: if the first RPC reached the server and was applied but
// the reply was lost, a retry will see the bumped version and return
// ErrVersion. We cannot distinguish that from a "lost race with another
// Clerk" case, so any ErrVersion observed on a retry is reported to the
// caller as ErrMaybe. An ErrVersion observed on the first attempt is
// reported as-is, because it unambiguously means the Put was not
// applied.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	firstTry := true
	for {
		reply := rpc.PutReply{}
		if ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) {
			if !firstTry && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		firstTry = false
		time.Sleep(retryBackoff)
	}
}
