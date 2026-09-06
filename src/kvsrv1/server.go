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

type item struct {
	value   string
	version rpc.Tversion
}
type KVServer struct {
	mu sync.Mutex

	data map[string]item
}

func MakeKVServer() *KVServer {
	kv := &KVServer{data: make(map[string]item)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	key := args.Key

	kv.mu.Lock()
	defer kv.mu.Unlock()

	curr, ok := kv.data[key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = curr.value
	reply.Version = curr.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	key := args.Key
	version := args.Version
	value := args.Value

	kv.mu.Lock()
	defer kv.mu.Unlock()

	curr, ok := kv.data[key]
	if !ok && version == 0 { // create new key
		kv.data[key] = item{value, 1}
		reply.Err = rpc.OK
		return
	}
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	if curr.version != version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.data[key] = item{value, curr.version + 1}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
