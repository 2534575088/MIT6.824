package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ----------------------------------------------------结构体定义部分------------------------------------------------------

// Op 结构体表示客户端提交到 Raft 日志的操作命令
// 包括操作类型、键值、客户端 ID 和请求序号，用于支持幂等性与状态机更新
// 注意字段首字母大写，以确保可以通过 RPC 编码
type Op struct {
	SeqId    int    // 客户端请求序号（配合 ClientId 实现幂等）
	Key      string // 操作的键
	Value    string // 操作的值（Put/Append）
	ClientId int64  // 客户端唯一标识（Clerk 创建时随机生成）
	Index    int    // Raft 返回的日志 index（可选）
	OpType   string // 操作类型："Put", "Append", "Get"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft         // 当前节点对应的 Raft 实例
	applyCh chan raft.ApplyMsg // 从 Raft 层传入的已提交日志项
	dead    int32              // 标志当前服务器是否已关闭（Kill）

	maxraftstate int // 超出该限制触发 snapshot；若为 -1，则不触发

	// KV Server 自身状态
	seqMap map[int64]int // 记录每个客户端最后处理的 SeqId，用于幂等检测
	waitChMap map[int]chan Op // 每条日志 index 对应一个 channel，等待响应
	kvPersist map[string]string // 实际存储的 Key-Value 数据

	lastIncludeIndex int // 当前快照所包含的最大日志 index
}

// -------------------------------------------------初始化(Start)部分------------------------------------------------------
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) // 注册 Op 类型，使其能被 Raft 持久化编码

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	kv.lastIncludeIndex = -1

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandlerLoop() // 启动后台 goroutine，持续处理 Raft 提交
	return kv
}

// ------------------------------------------------------Rpc部分---------------------------------------------------------

// 处理客户端 Get 请求，若为当前 leader，则提交到 Raft 并等待应用完成；否则立即返回错误
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: "Get", Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// 处理 Put 和 Append 请求，流程与 Get 相似，但提交后会更新状态机
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	
	select{
	case replyOp := <-ch:
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	defer timer.Stop()
}

// ------------------------------------------------------Loop部分--------------------------------------------------------

// 从 applyCh 不断读取 Raft 已提交的日志条目并应用到状态机
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				// 传来的信息快照已经存储了
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}

				index := msg.CommandIndex
				op := msg.Command.(Op)

				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value

					case "Append":
						kv.kvPersist[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				// 将返回的ch返回waitCh
				kv.getWaitCh(index) <- op
			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				// 判断此时有没有竞争
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照的数据
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

// ------------------------------------------------------持久化部分--------------------------------------------------------

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot!!!", kv.me)
	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

//-------------------------------------------------------utils部分--------------------------------------------------------

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 检查该请求是否是重复的（即是否已处理过）
func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

// 获取给定日志 index 的 wait channel，如果不存在就新建
func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}
