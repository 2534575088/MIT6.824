package kvraft

import (
	"6.824/labrpc"         // 使用 labrpc 模拟 RPC 网络通信
	"crypto/rand"          // 用于生成安全随机数（clientId）
	"math/big"             // 用于生成大数随机 clientId
	mathrand "math/rand"   // 用于普通伪随机数（初始化 leaderId）
)

// Clerk 代表一个客户端实例，负责发起对 KV 服务的操作请求
type Clerk struct {
	servers []*labrpc.ClientEnd // 所有已知的 KVServer 节点
	seqId int // 当前请求的序号（用于去重和幂等处理）
	leaderId int // 上一次请求成功的server
	clientId int64 // 客户端全局唯一标识
}

// nrand 生成一个高位随机整数，用作唯一的 clientId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 初始化并返回一个新的 Clerk 客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers   // 设置可用的服务端列表
	ck.clientId = nrand()  // 生成唯一客户端 ID
	ck.leaderId = mathrand.Intn(len(ck.servers)) // 随机猜测一个初始 leader
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
// Get 从 KV 集群中读取某个键的值
// 如果该 key 不存在，返回空字符串
func (ck *Clerk) Get(key string) string {
	ck.seqId++ // 每次请求都递增序号 确保唯一性
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		SeqId:  ck.seqId,
	}
	serverId := ck.leaderId // 从上次成功的 leader 开始尝试，提高成功率
	
	for {
		reply := GetReply{}

		// 尝试向当前server发起RPC调用
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				// key 不存在，不是错误，更新 leaderId 并返回空串
				ck.leaderId = serverId
				return ""
			} else if reply.Err == OK {
				// 正常获取成功，更新 leaderId 并返回结果
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				// server 不是 leader，尝试下一个节点
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 网络错误或调用失败，也尝试下一个 server
		serverId = (serverId + 1) % len(ck.servers)
	}
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
// PutAppend 是 Put 和 Append 的通用逻辑实现
// op 参数是 "Put" 或 "Append"
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqId++ // 请求序列自增
	serverId := ck.leaderId
	args := PutAppendArgs {
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		reply := PutAppendReply{}

		// 尝试向当前 server 发送 PutAppend 请求
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				// 操作成功，记录该 server 是 leader，返回
				ck.leaderId = serverId
				return 
			} else if reply.Err == ErrWrongLeader {
				// 该 server 不是 leader，尝试下一个 server
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// RPC 失败或 crash 也尝试下一个 server
		serverId = (serverId + 1) % len(ck.servers)
	}
}
// Put 封装 PutAppend，指定 op 为 "Put"
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
// Append 封装 PutAppend，指定 op 为 "Append"
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
