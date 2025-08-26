package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId int
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建一个 Clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

// Query 向 ShardCtrler 查询某个配置
func (ck *Clerk) Query(num int) Config {
	// 每次 RPC 请求增加 seqId
	ck.seqId++
	args := QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
	// 从上次猜测的 leader 开始请求
	serverId := ck.leaderId
	for {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)

		if ok {
			if reply.Err == OK {
				// 如果成功，说明 serverId 确实是 leader，下次还用它
				ck.leaderId = serverId
				return reply.Config
			}else if reply.Err == ErrWrongLeader{
				// 如果不是 leader，尝试下一个 server
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 如果节点 crash 或网络失败，也尝试下一个
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Join 请求：让新的服务器组加入集群
func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++
	args := JoinArgs{Servers: servers, ClientId : ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	
	for {
		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = ( serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave 请求：让一组服务器退出集群
func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId

	for {
		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Move 请求：将某个 shard 移动到指定的 GID
func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId

	for {
		reply := JoinReply{} // 同样使用 JoinReply 作为通用返回结构
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
