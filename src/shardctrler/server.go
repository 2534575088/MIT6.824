package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "time"
import "6.824/labgob"

const (
	// 操作类型常量
	JoinType = "join"
	LeaveType = "leave"
	MoveType = "move"
	QueryType = "query"

	// 每种操作的超时时间
	JoinOverTime = 100
	LeaveOverTime = 100
	MoveOverTime = 100
	QueryOverTime = 100

	// InvalidGid 表示无效的 group id
	// 在初始状态下，所有 shard 都属于 GID = 0
	InvalidGid = 0
)

//----------------------------------------------------结构体定义部分------------------------------------------------------
// ShardCtrler 表示一个分片控制器节点，它通过 Raft 保持容错和一致性
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // 从 Raft 层获取已提交日志的通道

	// 持久化应用状态
	configs []Config // indexed by config num

	// 辅助结构
	seqMap map[int64]int // 幂等性检测：clientId -> 最大seqId
	waitChMap map[int]chan Op // 等待 Raft 应用完成 ：日志 index -> channel
}

// 客户端请求封装成的操作
type Op struct {
	OpType      string        // 操作类型：join / leave / move / query
	ClientId    int64         // 客户端唯一 id
	SeqId       int           // 客户端请求序号
	QueryNum    int           // Query 请求参数
	JoinServers map[int][]string // Join 请求参数：GID -> 服务器列表
	LeaveGids   []int         // Leave 请求参数：要删除的 GID 列表
	MoveShard   int           // Move 请求参数：分片 id
	MoveGid     int           // Move 请求参数：目标组 GID
}

//------------------------------------------------------RPC部分---------------------------------------------------------
// Join RPC：添加新的 replica group 参数是 GID -> 服务器列表
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// 如果不是 leader 直接返回
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return 
	}

	// 封装成Op 交给 Raft 层 Start
	op := Op{OpType: JoinType, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex,_,_ := sc.rf.Start(op)
	
	// 等待 Raft 应用结果
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex) // 防止内存泄漏
		sc.mu.Unlock()
	}()

	// 设置超时
	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	// 收到 Raft 应用完成的通知
	case replyOp := <-ch: 
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C: 
		reply.Err = ErrWrongLeader
	}
}

// Leave RPC：移除一组 replica group
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{OpType: LeaveType, SeqId: args.SeqId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(LeaveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// Move RPC：将某个 shard 分配到指定的 GID
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{OpType: MoveType, SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(MoveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// Query RPC：返回指定编号的配置
// 如果 num = -1 或超过最大编号，返回最新配置
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{OpType: QueryType, SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(QueryOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			// 查询操作可以直接读取 configs 返回
			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs) - 1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

//------------------------------------------------------utils部分--------------------------------------------------------

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
// Kill 停止该 ShardCtrler
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester 提供 Raft 引用，供测试使用
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// 幂等性检测
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

// 获取某个日志 index 对应的等待 channel
func (sc *ShardCtrler) getWaitCh(index int) chan Op{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

//-------------------------------------------------初始化(Start)部分------------------------------------------------------

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
// StartServer 创建并启动一个 ShardCtrler 节点
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	// 初始配置：config[0]，所有 shard 属于无效组 (0)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	// Raft 与序列化相关
	labgob.Register(Op{}) // 注册 Op 类型，Raft 日志中需要序列化
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// 初始化辅助结构
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	// 启动后台协程，处理 Raft 提交的日志
	go sc.applyMsgHandlerLoop()
	return sc
}

//------------------------------------------------------Loop部分--------------------------------------------------------

// Raft 日志应用处理循环
func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for{
		select{
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)

				// 去重：如果已经执行过该请求，则跳过
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveGid, op.MoveShard))
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}
				sc.getWaitCh(index) <- op
			}
		}
	}
}

// ------------------------------------------------------handler部分-----------------------------------------------------

// JoinHandler：为新加入的 GID 分配分片，尽量负载均衡
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newGroups := make(map[int][]string)

	// 复制原有group
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	// 添加新 group
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	// 初始化 group -> shard 数量映射
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	// 如果没有 group，直接返回
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}

	// 需要负载均衡
	return &Config{
		Num: len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards),
		Groups:newGroups,
	}
}

// LeaveHandler：删除 group，并把其分片重新分配
func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	// 复制 group
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	// 删除指定 group
	for _, leaveGid := range gids {
		delete(newGroups, leaveGid)
	}

	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards

	// 初始化 group 负载
	for gid := range newGroups {
		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}
	}

	// 遍历旧分片分配
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if leaveMap[gid] {
				newShard[shard] = 0 // 删除组的分片置为无效
			} else {
				GroupMap[gid]++
			}
		}
	}

	// 如果全删了
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

// MoveHandler：强制指定某个 shard 属于某个 gid
func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{}}

	// 复制分片
	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}
	newConfig.Shards[shard] = gid // 强制改动

	// 复制组信息
	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}
	return &newConfig
}

//------------------------------------------------------负载均衡部分------------------------------------------------------

// loadBalance 将 shard 尽量均匀分配给 group
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(GroupMap)

	// 释放多余分片
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		if GroupMap[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := GroupMap[overLoadGid] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	// 为分片不足的组分配空闲 shard
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		if GroupMap[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGid {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}
	}
	return lastShards
}

// sortGroupShard：按分片数量升序排列 GID（负载小的在前，方便分配）
func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)
	gidSlice := make([]int, 0, length)

	for gid := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}

	// 冒泡排序：先按 shard 数量，再按 gid 大小
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] ||
				(GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

// moreAllocations：判断第 i 个组是否应该分配 (ave+1) 个分片
// 比如：10 个分片，4 个组，ave=2，remainder=2，前 2 个组分配 3 个，其余 2 个组分配 2 个
func moreAllocations(length int, remainder int, i int) bool {
	if i < length-remainder {
		return true
	} else {
		return false
	}
}