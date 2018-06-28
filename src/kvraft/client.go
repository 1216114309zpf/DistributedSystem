package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

currentId := 0
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
        clientId int64
        mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
        ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//return ""
        ck.mu.Lock()
        commandId := currentId
        currentId++
        ck.mu.Unlock()

        args := GetArgs{key, ck.clientId, commandId}
        value := ""
        for {
           flag := false
           for i:=0; i<len(ck.servers); i++ {
                 reply := new(GetReply)
                 ok := ck.servers[i].Call("KVServer.Get", &args,reply)
                 if ok {
                     if !reply.WrongLeader {
                         
                         if reply.Err == "" {
                             value := reply.Value
                             flag=true
                             break
                         }
                     }
                 }
           }
           if flag {
               break
           }
        }
        return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
        args := PutAppendArgs{key, value, op, ck.clientId, nrand()}
        for {
           flag := false
           for i:=0; i<len(ck.servers); i++ {
                 reply := new(PutAppendReply)
                 ok := ck.servers[i].Call("KVServer.PutAppend", &args,reply)
                 if ok {
                     if !reply.WrongLeader {
                         if reply.Err == "" 
                             flag=true
                             break
                         }
                     }
                 }
           }
           if flag {
               break
           }
        }
        return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
