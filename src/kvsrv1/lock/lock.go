package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	name     string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, name: lockname}
	ck.Put(lockname, "", 0) // to avoid handling ErrNoKey in methods
	return lk
}

func (lk *Lock) Acquire() {
	value, version, _ := lk.ck.Get(lk.name)
	for value != "" {
		value, version, _ = lk.ck.Get(lk.name)
	}

	clientID := kvtest.RandValue(8)
	err := lk.ck.Put(lk.name, clientID, version)
	switch err {
	case rpc.ErrVersion:
		lk.Acquire()
		return
	case rpc.ErrMaybe:
		value, _, _ := lk.ck.Get(lk.name)
		if value != clientID {
			lk.Acquire()
			return
		}
	}
	// Set the clientID only after a successful acquisition.
	// Otherwise, it might hold the clientID from one of the
	// concurrent unsuccessful acquisition attempts
	lk.clientID = clientID
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.name)
	if value == "" {
		return
	}
	if value != lk.clientID {
		return
	}

	lk.ck.Put(lk.name, "", version)
}
