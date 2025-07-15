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

// InstallSnapshotState 安装快照的状态
type InstallSnapshotState int

// 枚举节点的类型：跟随者 竞争者 领导者
const (
	Follower Status = iota
	Candidate
	Leader
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

const (
	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 20
	MinVoteTime = 15

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 12
	AppliedSleep = 3
)


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
	currentTerm  int        // 当前任期
	votedFor     int        // 记录当前任期票投给谁
	logs         []LogEntry // 日志条目数组 包含状态机要执行的指令集 以及收到领导时的任期号

	// 所有的servers经常修改的
	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 状态机中已知的被提交的日志条目的索引值（初始化为0 持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值

	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int 
	matchIndex []int

	applyChan chan ApplyMsg // 用来写入通道

	status     Status 
	voteNum    int
	votedTimer time.Time

	// 2D中用于传入快照点
	lastIncludeIndex int
	lastIncludeTerm  int
}

// LogEntry 表示 Raft 日志中的一个条目
type LogEntry struct {
	Term    int         // 日志条目的任期
	Command interface{} // 要应用到状态机的命令
}

// --------------------------------------------------------RPC参数部分----------------------------------------------------


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
}


// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // Leader 的任期
	LeaderId     int        // Leader 的 ID，用于 Follower 重定向客户端
	PrevLogIndex int        // 紧邻新日志条目之前的日志条目的索引 (1-based)
	PrevLogTerm  int        // PrevLogIndex 处的日志条目的任期
	Entries      []LogEntry // 要存储的日志条目 (心跳时为空)
	LeaderCommit int        // Leader 的 commit index指的是最后一个被大多数机器都复制的日志Index
}

// AppendEntriesReply 是 AppendEntries RPC 的回复
type AppendEntriesReply struct {
	Term        int         // 当前任期，用于 Leader 更新自身
	Success     bool        // 如果 Follower 包含 PrevLogIndex 处的条目且任期匹配，则为 true
	UpNextIndex int         // 用于 Leader 更新其 nextIndex[i] (日志匹配优化)
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
}

type InstallSnapshotReply struct {
	Term int
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

	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.commitIndex = 0 // 已知已提交的最高日志索引 (1-based)，初始为 0 (虚拟条目)
	rf.lastApplied = 0 // 已应用到状态机的最高日志索引 (1-based)，初始为 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()
	
	// 从持久化状态中恢复
	rf.readPersist(persister.ReadRaftState())
	
	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
	
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// --------------------------------------------------------ticker部分----------------------------------------------------

// 触发选举
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		// 时间过期发起选举 此处的流程为 
		// 每次votedTimer如果小于在sleep睡眠之前定义的时间，
		// 就代表没有votedTimer没被更新为最新的时间，则发起选举
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 转变状态
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			rf.sendElection()
			rf.votedTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid : true,
				SnapshotValid: false,
				CommandIndex: rf.lastApplied,
				Command: rf.restoreLog(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}
}

//----------------------------------------------leader选举部分------------------------------------------------------------

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {

						//fmt.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(i)

	}

}

// RequestVote
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前节点在数据上不冲突时就要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期结果：任期大于当前节点 进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || 
		// paper中的第二个条件votedFor is null
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {

		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}
}

//----------------------------------------------日志增量部分------------------------------------------------------------
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			// installSnapshot，如果rf.nextIndex[i]-1小于等于lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
			// 同时要注意的是比快照还小时，已经算是比较落后
			if rf.nextIndex[server] - 1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &args, &reply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}

				if reply.Success {
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 外层遍历下标是否满足,从快照最后开始反向进行
					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex + 1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i== rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 大于一半，且因为是从后往前，一定会大于原本commitIndex
						if sum >= len(rf.peers)/2 + 1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}
		}(index)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 自身的快照Index比发过来的prevLogIndex还大，所以返回冲突的下标加1(原因是冲突的下标用来更新nextIndex，nextIndex比Prev大1
	// 返回冲突下标的目的是为了减少RPC请求次数
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	// 如果自身最后的快照日志比prev小说明中间有缺失日志，such 3、4、5、6、7 返回的开头为6、7，而自身到4，缺失5
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex()
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// 进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	return
}

//----------------------------------------------持久化（persist)部分---------------------------------------------------------
//
// save Raft's persistent status to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
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
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
	  	rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs {
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据不合适
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog =append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyChan <- msg
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}


	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	// 更新快照下标/任期
	if index == rf.getLastIndex() + 1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs

	// apply了快照就应该重置commitIndex、lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}


//----------------------------------------------Raft Public API---------------------------------------------------------

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}


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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.status != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		return index, term, true
	}
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
	
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}