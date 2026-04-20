package lock

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// Lock invariant
//
// The lock's state is stored in the KV server under the key `lockname`.
// The stored value encodes the current holder:
//
//   - ""                   -> the lock is free
//   - <non-empty clientID> -> the lock is held by that clientID
//
// Acquire performs a conditional Put to transition the value from "" (or
// ErrNoKey) to the caller's clientID at the current version. Release
// performs a conditional Put back to "" at the current version, and only
// if the caller is the current holder.
//
// Because Clerk.Put may return ErrMaybe when a reply is lost on the
// unreliable network, both Acquire and Release must tolerate the case
// where their own Put actually succeeded (or didn't): on the next
// iteration a fresh Get disambiguates by showing the current holder.

// backoffMin / backoffMax bound the jittered wait between retries. A
// little randomisation reduces thundering-herd behaviour when many
// clients contend for the same lock.
const (
	backoffMin = 50 * time.Millisecond
	backoffMax = 150 * time.Millisecond
)

type Lock struct {
	lockname string
	ck       kvtest.IKVClerk
	clientID string
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	return &Lock{
		ck:       ck,
		lockname: lockname,
		clientID: kvtest.RandValue(8),
	}
}

func (lk *Lock) Acquire() {
	for {
		holder, version, err := lk.ck.Get(lk.lockname)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			// proceed
		default:
			log.Fatalf("lock Acquire: unexpected Get err %q", err)
		}

		// If we already hold the lock (e.g. a prior Put returned
		// ErrMaybe but actually landed), we're done.
		if holder == lk.clientID {
			return
		}

		// Someone else holds it; back off and retry.
		if holder != "" {
			sleepJitter()
			continue
		}

		switch lk.ck.Put(lk.lockname, lk.clientID, version) {
		case rpc.OK:
			return
		case rpc.ErrVersion, rpc.ErrMaybe, rpc.ErrNoKey:
			// ErrVersion -> lost the race; retry.
			// ErrMaybe   -> unclear whether we hold it; the next Get
			//               will disambiguate.
			// ErrNoKey   -> defensive; should not occur since we only
			//               Put with version==0 when the key is absent.
			sleepJitter()
		default:
			log.Fatalf("lock Acquire: unexpected Put err")
		}
	}
}

func (lk *Lock) Release() {
	for {
		holder, version, err := lk.ck.Get(lk.lockname)
		switch err {
		case rpc.OK:
		case rpc.ErrNoKey:
			// Key was never created; nothing to release.
			return
		default:
			log.Fatalf("lock Release: unexpected Get err %q", err)
		}

		// If we are not the current holder, a prior Put in this
		// Release call must have landed (even if its reply was lost
		// and we received ErrMaybe). Another client may have already
		// acquired; in any case our release is complete.
		if holder != lk.clientID {
			return
		}

		switch lk.ck.Put(lk.lockname, "", version) {
		case rpc.OK:
			return
		case rpc.ErrMaybe, rpc.ErrVersion:
			// ErrMaybe   -> reply lost; loop again. If the Put landed,
			//               the next Get will show holder != us and we
			//               exit; otherwise we retry with the current
			//               version.
			// ErrVersion -> shouldn't happen while we hold the lock,
			//               but retry defensively.
			sleepJitter()
		default:
			log.Fatalf("lock Release: unexpected Put err")
		}
	}
}

func sleepJitter() {
	d := backoffMin + time.Duration(rand.Int63n(int64(backoffMax-backoffMin)))
	time.Sleep(d)
}
