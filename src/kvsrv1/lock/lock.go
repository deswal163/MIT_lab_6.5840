package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	lockname string
	ck       kvtest.IKVClerk
	clientID string
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	clientID := kvtest.RandValue(8)
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		clientID: clientID,
	}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		lockClientID, lockVersion, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK || err == rpc.ErrNoKey {
			if lockClientID != "" && lockClientID != lk.clientID {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			ok := lk.ck.Put(lk.lockname, lk.clientID, lockVersion)
			if ok != rpc.OK {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		_, lockVersion, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ok := lk.ck.Put(lk.lockname, "", lockVersion)
		if ok != rpc.OK && ok != rpc.ErrNoKey {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}

}
