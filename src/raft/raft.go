package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// Status 节点的角色
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

// 枚举节点的类型：跟随者 竞争者 领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

// VoteState 投票过程中的状态
const (
	Normal VoteState = iota //投票过程正常
	Killed  //Raft节点已终止
	Expire  //投票(消息\竞选者）过期
	Voted   //本Term内已经投过票
)

// AppendEntriesState 追加日志过程中的状态
const (
	AppNormal    AppendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	//不再使用以下自定义状态，Raft 论文中没有直接对应的状态
	// AppRepeat                              // 追加重复 (2B
	// AppCommited                            // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B
)

// ApplyMsg 是 Raft 节点向服务层发送的已提交日志条目或快照消息
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有的servers拥有的变量
	currentTerm int // 当前任期
	votedFor int // 记录当前任期票投给谁
	logs []LogEntry // 日志条目数组 包含状态机要执行的指令集 以及收到领导时的任期号

	// 所有的servers经常修改的
	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 状态机中已知的被提交的日志条目的索引值（初始化为0 持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值

	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex []int 
	matchIndex []int

	status Status 
	overtime time.Duration
	timer *time.Ticker

	applyChan chan ApplyMsg
	applyCond *sync.Cond    // Condition variable to signal the applier goroutine
}

// LogEntry 表示 Raft 日志中的一个条目
type LogEntry struct {
	Term    int         // 日志条目的任期
	Command interface{} // 要应用到状态机的命令
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // Leader 的任期
	LeaderId     int        // Leader 的 ID，用于 Follower 重定向客户端
	PrevLogIndex int        // 紧邻新日志条目之前的日志条目的索引 (1-based)
	PrevLogTerm  int        // PrevLogIndex 处的日志条目的任期
	Entries      []LogEntry // 要存储的日志条目 (心跳时为空)
	LeaderCommit int        // Leader 的 commitIndex
}

// AppendEntriesReply 是 AppendEntries RPC 的回复
type AppendEntriesReply struct {
	Term        int                // 当前任期，用于 Leader 更新自身
	Success     bool               // 如果 Follower 包含 PrevLogIndex 处的条目且任期匹配，则为 true
	AppState    AppendEntriesState // 自定义状态，提供更详细的回复信息
	UpNextIndex int                // 用于 Leader 更新其 nextIndex[i] (日志匹配优化)
}

// --- Raft Public API ---

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.status == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
	    d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// --- RequestVote RPC ---

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!

type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateId  int // 请求投票的候选者 ID
	LastLogIndex int // 候选者最后日志条目的索引 (1-based)
	LastLogTerm  int // 候选者最后日志条目的任期
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 如果竞选者任期比自己的任期还短，那就不投票，返回false
// 如果当前节点的votedFor为空，且竞选者的日志条目跟收到者的一样新则把票投给该竞选者
type RequestVoteReply struct {
	Term        int       // 当前任期，用于候选者更新自身
	VoteGranted bool      // true 表示候选者获得投票
	VoteState   VoteState // 自定义状态，提供更详细的回复信息
}


// RequestVote
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前节点在数据上不冲突时就要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = rf.currentTerm // 返回当前任期，即使已终止
		reply.VoteGranted = false
		return 
	}

	// 1. 如果 args.Term < currentTerm，则返回 false (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果 RPC 请求或响应包含 Term T > currentTerm：设置 currentTerm = T，转换为 Follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 新任期开始，重置 votedFor
		rf.persist() // (2C) Persist state change
	}

	/*
	Raft 论文指出：
	“如果 votedFor 为空或为 candidateId，并且候选者的日志至少和接收者的日志一样新，则授予投票。”
	“新旧定义： Raft 通过比较日志中最后一条条目的索引和任期来判断两条日志中哪条更旧。
	如果日志的最后条目任期不同，则任期较大的日志更新。如果日志的最后条目任期相同，则日志较长的更新。”
	*/

	// 2. 如果 votedFor 为空或为 candidateId，并且候选者的日志至少和接收者的日志一样新，则授予投票 (§5.2, §5.4)
	lastLogIndex := len(rf.logs) - 1 // 最后一个实际日志条目的索引 (1-based)
	lastLogTerm := rf.logs[lastLogIndex].Term // Safe because rf.logs is initialized with dummy entry

	candidateLogIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		candidateLogIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			candidateLogIsUpToDate = true
		}
	}

	// 只有在未投票或已投票给该候选者 (幂等性)，并且候选者日志足够新时才授予投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateLogIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// 授予投票后，重置选举计时器，表示有活跃的选举在进行
		rf.timer.Reset(rf.overtime)
		rf.persist() // (2C) Persist state change
	} else {
		// 拒绝投票
		reply.VoteState = Voted // 或 Expire (如果日志不新)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// No timer reset here if denying vote, unless args.Term == rf.currentTerm (which is implied)
		// and it's a valid candidate in same term that you just can't vote for.
		// However, it's safer to only reset on granted votes or valid heartbeats.
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 向指定服务器发送 RequestVote RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCount *int) {
	if rf.killed(){
		return
	}

	// IMPORTANT: 移除无限重试循环。RPC 失败由 Raft 的超时机制处理。
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// Process reply only if RPC was successful and the current node hasn't changed its term/status
	// since the RPC was sent.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 关键检查：如果当前任期已改变 (如已选出 Leader 或收到更高任期的 RPC)，则此回复已过时，应丢弃。
	if args.Term != rf.currentTerm || rf.status != Candidate {
		return 
	}

	if !ok {
		// RPC 失败 (例如，网络问题，服务器宕机)。不重试，直接返回。
		return 
	}


	// 1. 如果 RPC 响应包含 Term T > currentTerm：设置 currentTerm = T，转换为 Follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime) // 转换为 Follower 时重置计时器
		rf.persist() // 2C: 状态改变需要持久化
		return
	}

	// 处理当前任期的有效回复
	if reply.VoteGranted {
		*voteCount++
		// 如果候选者在当前任期内从多数服务器获得投票：成为 Leader
		if *voteCount >= (len(rf.peers)/2)+1 {
			// 确保只在仍是 Candidate 状态时才转换为 Leader
			if rf.status == Candidate {
				rf.status = Leader
				// 初始化 nextIndex 和 matchIndex
				lastLogIndex := len(rf.logs) - 1 // 最后一个实际日志条目的索引
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = lastLogIndex + 1 // Leader 认为 Follower 需要从其最后一条日志的下一条开始
					rf.matchIndex[i] = 0               // 初始时未匹配任何日志
				}
				// 作为新 Leader，立即发送心跳以确立领导地位
				rf.timer.Reset(HeartBeatTimeout)
				// fmt.Printf("Server %d became leader for term %d\n", rf.me, rf.currentTerm) // Debug
			}
		}
	}
	return
}

// --- Start Agreement ---

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 客户端调用此函数以开始对新日志条目达成一致
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false // 默认不是 Leader

	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return index, term, false // 如果不是 Leader，返回 false
	}

	isLeader = true

	// 初始化日志条目 并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	// 返回的是 1-based 索引，如果 logs[0] 是虚拟条目，则 len(rf.logs) 是下一个可用索引，所以 -1 是当前新条目的索引
	index = len(rf.logs) - 1 
	term = rf.currentTerm
	rf.persist() // (2C) Persist state change

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// Kill 方法用于终止 Raft 节点
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// --- Ticker Goroutine ---

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 启动一个独立的 Goroutine 来应用日志，避免阻塞 RPC 处理器
	go rf.applier()

	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()

			switch rf.status {
			case Follower:
				// Follower 计时器超时，转换为 Candidate
				rf.status = Candidate
				// fallthrough 到 Candidate 逻辑
				fallthrough
			case Candidate:
				// Candidate: 开始新一轮选举
				rf.currentTerm += 1
				rf.votedFor = rf.me // 投票给自己
				rf.persist() // 2C: 状态改变需要持久化

				votedCount := 1 // 统计自身的票数

				// 每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				// 对自身以外的节点请求投票
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:        rf.currentTerm,
						CandidateId: rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm: rf.logs[len(rf.logs)-1].Term, // 安全，因为 logs[0] 是虚拟条目
					}

					voteReply := RequestVoteReply{}
					// 在新的 Goroutine 中发送 RPC，避免阻塞
					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedCount)
				}

			case Leader:
				// Leader: 发送心跳/日志同步
				rf.timer.Reset(HeartBeatTimeout) // Leader 每隔 HeartBeatTimeout 发送一次心跳

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					// 计算 PrevLogIndex 和 PrevLogTerm
					// nextIndex[i] 是 Leader 认为 Follower 需要的下一个日志条目的索引 (1-based)
					// PrevLogIndex 是 nextIndex[i] 的前一个日志条目的索引
					prevLogIndex := rf.nextIndex[i] - 1
					prevLogTerm := rf.logs[prevLogIndex].Term // 安全，因为 prevLogIndex >= 0 (至少是虚拟条目)

					// 构造 AppendEntries RPC 参数
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						LeaderCommit: rf.commitIndex,
					}

					// 包含从 nextIndex[i] 开始到 Leader 日志末尾的所有条目
					// 如果 nextIndex[i] 等于 len(rf.logs)，则 Entries 为空，即心跳
					args.Entries = rf.logs[rf.nextIndex[i]:]

					reply := AppendEntriesReply{}
					// 在新的 Goroutine 中发送 RPC
					go rf.sendAppendEntries(i, &args, &reply) // 不再传递 appendNums
				}

				// Leader 提交日志的逻辑：
				// 如果存在一个 N > commitIndex，并且大多数 matchIndex[i] >= N，
				// 并且 logs[N].Term == currentTerm，那么设置 commitIndex = N。
				// 从 commitIndex + 1 开始检查，直到 Leader 自己的最新日志。
				n := rf.commitIndex + 1
				for n <= len(rf.logs)-1 { // 遍历所有可能提交的日志索引 (从 1-based 的 commitIndex + 1 开始)
					// 确保该日志条目是当前 Leader 任期内的，这是 Raft 安全性规则
					if rf.logs[n].Term != rf.currentTerm {
						n++
						continue
					}

					// 统计有多少个 Follower 的 matchIndex 达到了或超过 N
					count := 1 // Leader 自己已经匹配了
					for i := range rf.peers {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= n {
							count++
						}
					}

					// 如果多数派已经复制了该日志条目
					if count >= (len(rf.peers)/2)+1 {
						rf.commitIndex = n // 更新 commitIndex
						rf.applyCond.Signal() // 唤醒 applier Goroutine 应用日志
						n++                   // 继续检查下一个日志条目
					} else {
						break // 未达到多数派，停止检查
					}
				}
			}
			rf.mu.Unlock() // 解锁
		}
	}
}

// applier Goroutine 负责将已提交的日志应用到状态机
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		// 如果没有新的日志需要应用 则等待
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				 return
			}
		}

		// 应用日志
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg {
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				// 注意：如果 logs[0] 是虚拟条目，这里直接用 lastApplied 作为索引
				Command: rf.logs[rf.lastApplied].Command,
			}
			// 释放锁，以便其他 Goroutine 可以操作 Raft 状态，同时发送 ApplyMsg
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
			rf.mu.Lock()
		}
	}
}

// --- Make Raft ---

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 创建一个新的 Raft 服务器
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu) // 初始化条件变量

	// 初始化 Raft 状态
	rf.currentTerm = 0
	rf.votedFor = -1

	// 初始化 logs 数组，包含一个虚拟条目在索引 0。
	// 这使得日志索引与 Raft 论文中的 1-based 索引保持一致。
	// 实际的日志条目将从索引 1 开始。
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0, Command: nil} // 虚拟条目，任期为 0

	rf.commitIndex = 0 // 已知已提交的最高日志索引 (1-based)，初始为 0 (虚拟条目)
	rf.lastApplied = 0 // 已应用到状态机的最高日志索引 (1-based)，初始为 0

	// nextIndex 和 matchIndex 仅与 Leader 相关，在成为 Leader 时初始化
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)


	// 播种随机数生成器
	rand.Seed(time.Now().UnixNano())

	// 从持久化状态中恢复
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker goroutine 来触发选举或发送心跳
	go rf.ticker()


	return rf
}

// --- AppendEntries RPC ---

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return 
	}

	// IMPORTANT: 移除无限重试循环 RPC 失败由 Raft 的超时机制和 Leader 的 nextIndex 调整来处理
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// 必须在加在这里否则加载前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前节点已不是 Leader 或任期已改变，则此回复已过时，应丢弃。
	if args.Term != rf.currentTerm || rf.status != Leader {
		return
	}

	if !ok {
		// RPC 失败 (例如，网络问题，服务器宕机)。不重试。
		// Leader 应该在下次尝试时，根据 nextIndex 重新发送。
		// 这里不需要显式减小 nextIndex，因为 Follower 没收到，下次还是从 nextIndex 发。
		// 只有当 Follower 返回 Mismatch 时才需要调整 nextIndex。
		return
	}

	// 如果 RPC 响应包含 Term T > currentTerm：设置 currentTerm = T，转换为 Follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.persist()
		return
	}

	// 处理当前任期的有效回复
	if reply.Success { // Follower 成功匹配并追加了日志
		// 更新 matchIndex 和 nextIndex
		// matchIndex[server] 应该设置为 Follower 成功复制的最后一条日志的索引
		// nextIndex[server] 应该设置为 matchIndex[server] + 1
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] { // 只有当新的 matchIndex 更大时才更新
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// Leader 收到成功响应后，可以尝试更新 commitIndex
		// 这个逻辑放在 ticker 中集中处理更安全和清晰，避免竞态条件。
		// rf.updateCommitIndex() // 可以考虑在这里触发 commitIndex 更新
	} else { // reply.Success 为 false，表示日志不匹配或 Leader 任期过旧
		switch reply.AppState {
		case Mismatch:
			// Follower 告知 Leader 日志不匹配，并提供了 UpNextIndex 提示
			if reply.UpNextIndex < rf.nextIndex[server] { // 只有当 Follower 提示的索引更小时才更新
				rf.nextIndex[server] = reply.UpNextIndex
			}
			// 确保 nextIndex 至少为 1 (虚拟条目之后)
			if rf.nextIndex[server] <= 0 {
				rf.nextIndex[server] = 1
			}
			// fmt.Printf("Leader %d: Follower %d log mismatch, nextIndex adjusted to %d\n", rf.me, server, rf.nextIndex[server]) // Debug
		case AppOutOfDate:
			// 这种情况应该已经被 `if reply.Term > rf.currentTerm` 处理了
			// 如果走到这里，说明 reply.Term <= rf.currentTerm 且 AppOutOfDate，这通常不应该发生
			// 或者表示 Leader 的 Term 确实过时了，但这里已经处理过了。
		case AppKilled:
			// Follower 终止，Leader 无法与其通信，下次重试即可。
			// case AppCommited: // 移除这个自定义状态
		default:
			// 其他失败情况，简单地将 nextIndex 减 1 尝试回溯
			// 这是论文中没有优化的基本回溯策略
			if rf.nextIndex[server] > 1 { // 确保不会减到 0 或负数
				rf.nextIndex[server]--
			}
			// fmt.Printf("Leader %d: Follower %d generic failure, nextIndex decremented to %d\n", rf.me, server, rf.nextIndex[server]) // Debug
		}
	}
	return
}

// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 1. 如果 args.Term < currentTerm，则返回 false (§5.1)
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果 RPC 请求或响应包含 Term T >= currentTerm：设置 currentTerm = T，转换为 Follower (§5.1)
	// 任何有效的 AppendEntries RPC (包括心跳) 都应重置计时器并使节点保持 Follower 状态。
	if args.Term > rf.currentTerm { // 如果 Leader 的任期更高，更新自身任期
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 新任期重置 votedFor
		rf.persist() // 2C: 状态改变需要持久化
	}
	// 无论任期是否更高，只要是有效的 Leader 发来的 RPC，就重置计时器
	rf.votedFor = args.LeaderId // 记录 Leader ID
	rf.status = Follower        // 转换为 Follower
	rf.timer.Reset(rf.overtime) // 重置选举计时器

	// 2. 如果日志不包含 PrevLogIndex 处的条目，或者 PrevLogIndex 处的任期与 PrevLogTerm 不匹配，则返回 false (§5.3)
	// 索引是 1-based，所以 logs 数组的长度需要大于 PrevLogIndex
	if len(rf.logs)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.UpNextIndex = len(rf.logs) // 告诉 Leader 从 Follower 的日志末尾开始发送
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		// 优化：找到冲突任期的第一个索引，告知 Leader 从那里开始回溯
		conflictTerm := rf.logs[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.logs[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.UpNextIndex = conflictIndex // 告诉 Leader 从冲突任期的第一个索引开始发送
		return
	}

	// 3. 如果现有日志条目与新条目冲突 (相同索引但不同任期)，删除现有条目及其之后的所有条目 (§5.3)
	// 4. 追加 Leader 日志中所有不在 Follower 日志中的新条目
	if len(args.Entries) > 0 { // 只有当 Leader 实际发送了日志条目时才进行追加
		// 找到第一个冲突或新条目开始的位置
		// args.PrevLogIndex + 1 是 Leader 发送的第一个新条目的索引
		// newEntryIdx 是 args.Entries 中第一个需要追加的条目的索引
		newEntryIdx := 0
		for ; newEntryIdx < len(args.Entries); newEntryIdx++ {
			logIndex := args.PrevLogIndex + 1 + newEntryIdx
			if logIndex >= len(rf.logs) {
				// Follower 日志不够长， Leader 发送的条目是全新的
				break
			}
			if rf.logs[logIndex].Term != args.Entries[newEntryIdx].Term {
				// 发现冲突，从这里开始截断
				break
			}
		}

		// 如果有需要追加的新条目或需要截断的冲突条目
		if newEntryIdx < len(args.Entries) || (args.PrevLogIndex+1+newEntryIdx < len(rf.logs)) {
			// 截断冲突日志：删除从 (PrevLogIndex + 1 + newEntryIdx) 开始的所有条目
			rf.logs = rf.logs[:args.PrevLogIndex+1+newEntryIdx]
			// 追加 Leader 发送的新条目
			rf.logs = append(rf.logs, args.Entries[newEntryIdx:]...)
			rf.persist() // (2C) Persist state change
		}
	}

	// 5. 如果 LeaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		indexOfLastNewEntry := len(rf.logs) - 1 // Follower 当前日志的最后一个索引
		newCommitIndex := args.LeaderCommit
		if indexOfLastNewEntry < newCommitIndex { // 不能提交超出自身日志范围的条目
			newCommitIndex = indexOfLastNewEntry
		}
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal() // 唤醒 applier Goroutine 应用日志
		}
	}

	reply.Success = true
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	return
}