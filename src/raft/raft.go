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
	_ "6.824/labgob"
	"6.824/labrpc"
	_ "bytes"
	_ "fmt"
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

const (
	Normal VoteState = iota //投票过程正常
	Killed  //Raft节点已终止
	Expire  //投票(消息\竞选者）过期
	Voted   //本Term内已经投过票
)

const (
	AppNormal    AppendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppRepeat                              // 追加重复 (2B
	AppCommited                            // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B
)


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
}

type LogEntry struct {
	Term int
	Command interface{}
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term int 
	LeaderId  int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	AppState AppendEntriesState
}

// --- Raft Public API ---

// return currentTerm and whether this server
// believes it is the leader.
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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



// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!

type RequestVoteArgs struct {
	Term int 
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 如果竞选者任期比自己的任期还短，那就不投票，返回false
// 如果当前节点的votedFor为空，且竞选者的日志条目跟收到者的一样新则把票投给该竞选者
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	VoteState VoteState
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
		reply.Term = -1
		reply.VoteGranted = false
		return 
	}

	// 1. Reply false if args.Term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	/*
	Raft 论文指出：
	“如果 votedFor 为空或为 candidateId，并且候选者的日志至少和接收者的日志一样新，则授予投票。”
	“新旧定义： Raft 通过比较日志中最后一条条目的索引和任期来判断两条日志中哪条更旧。
	如果日志的最后条目任期不同，则任期较大的日志更新。如果日志的最后条目任期相同，则日志较长的更新。”
	*/
	// if rf.votedFor == -1 {
		
	// 	currentLogIndex := len(rf.logs) - 1
	// 	currentLogTerm := 0

	// 	if currentLogIndex >= 0 {
	// 		currentLogTerm = rf.logs[currentLogIndex].Term
	// 	}

	// 	if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
	// 		reply.VoteState = Expire
	// 		reply.VoteGranted = false
	// 		reply.Term = rf.currentTerm
	// 		return
	// 	}

	// 	rf.votedFor = args.CandidateId

	// 	reply.VoteState = Normal
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = true

	// 	rf.timer.Reset(rf.overtime)
	// } else {
	// 	reply.VoteState = Voted
	// 	reply.VoteGranted = false

	// 	if rf.votedFor != args.CandidateId {
	// 		return
	// 	} else {
	// 		rf.status = Follower
	// 	}

	// 	rf.timer.Reset(rf.overtime)
	// }
	// return


	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term // Safe because rf.logs is initialized with dummy entry

	candidateLogIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		candidateLogIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			candidateLogIsUpToDate = true
		}
	}

	// Only vote if not already voted in this term, or if already voted for this candidate (idempotent)
	// and candidate's log is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateLogIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// Importantly, reset timer when granting a vote, as it means a valid election is ongoing
		rf.timer.Reset(rf.overtime)
	} else {
		// Deny vote
		reply.VoteState = Voted // Or Expire if log not up-to-date (handled by candidateLogIsUpToDate check)
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed(){
		return false
	}

	// IMPORTANT: Remove the infinite retry loop.
	// If the RPC fails, it's handled by Raft's timeout mechanism.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// Process reply only if RPC was successful and the current node hasn't changed its term/status
	// since the RPC was sent.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Crucial check: If currentTerm has changed (e.g., elected leader or received higher term RPC)
	// since this RPC was sent, then this reply is stale. Discard it.
	if args.Term != rf.currentTerm || rf.status != Candidate {
		// This RPC reply is from an old election round or we are no longer a candidate.
		return false
	}

	if !ok {
		// RPC failed (e.g., network issue, server down). Don't retry, just return.
		return false
	}

	// 1. If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime) // Reset timer when transitioning to follower
		return false // This reply is for an old term, not relevant for current election
	}

	// Process valid reply for current term
	if reply.VoteGranted {
		*voteNums++
		// If candidate receives votes from majority of servers for current term: becomes leader
		if *voteNums >= (len(rf.peers)/2)+1 {
			// Ensure we transition to Leader only once per election
			if rf.status == Candidate { // Only transition if still a Candidate
				rf.status = Leader
				// Initialize nextIndex for all followers to leader's last log index + 1
				// And matchIndex to 0
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				lastLogIndex := len(rf.logs) - 1 // Last actual log entry index (including dummy at 0)
				for i := range rf.peers {
					rf.nextIndex[i] = lastLogIndex + 1
					rf.matchIndex[i] = 0 // Initially no entries matched
				}
				// As a new leader, send immediate heartbeats to assert leadership
				rf.timer.Reset(HeartBeatTimeout)
				// fmt.Printf("Server %d became leader for term %d\n", rf.me, rf.currentTerm) // Debug
			}
		}
	}
	// else if reply.VoteState == Expire { // This could indicate log not up-to-date
	// 	// Do nothing special, just don't get the vote.
	// }
	return ok

	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// for !ok {
	// 	// 失败重传
	// 	if rf.killed() {
	// 		return false
	// 	}
	// 	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	// }

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// if args.Term < rf.currentTerm {
	// 	return false
	// }

	// // 消息过期有两种情况:
	// // 1.是本身的term过期了比节点的还小
	// // 2.是节点日志的条目落后于节点了
	// switch reply.VoteState {
	// case Expire:
	// 	{
	// 		rf.status = Follower
	// 		rf.timer.Reset(rf.overtime)
	// 		if reply.Term > rf.currentTerm {
	// 			rf.currentTerm = reply.Term
	// 			rf.votedFor = -1
	// 		}
	// 	}
	// case Normal, Voted:
	// 	//根据是否同意投票，收集选票数量
	// 	if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
	// 		*voteNums++
	// 	}

	// 	// 票数超过一半
	// 	if *voteNums >= (len(rf.peers) / 2) + 1 {
	// 		*voteNums = 0
	// 		if rf.status == Leader {
	// 			return ok
	// 		}

	// 		rf.status = Leader
	// 		rf.nextIndex = make([]int, len(rf.peers))
	// 		for i, _ := range rf.nextIndex {
	// 			rf.nextIndex[i] = len(rf.logs) + 1
	// 		}
	// 		rf.timer.Reset(HeartBeatTimeout)
	// 	}
	// case Killed:
	// 	return false
	// }
	// return ok
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
//
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed(){
				return
			}
			rf.mu.Lock()
			switch rf.status {
			case Follower:
				rf.status = Candidate
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1

				rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				for i := 0; i < len(rf.peers); i++ {
					if i== rf.me {
						continue
					}

					voteArgs := RequestVoteArgs {
						Term: rf.currentTerm,
						CandidateId: rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm: 0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			case Leader:
				appendNums := 1
				rf.timer.Reset(HeartBeatTimeout)
				for i := 0; i < len(rf.peers); i++ {
					if i== rf.me {
						continue
					}
					AppendEntriesArgs := AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: 0,
						PrevLogTerm: 0,
						Entries: nil,
						LeaderCommit: rf.commitIndex,
					}

					AppendEntriesReply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &AppendEntriesArgs, &AppendEntriesReply, &appendNums)
				}
			}

			rf.mu.Unlock()
		}

	}
}

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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Term: 0, Command: nil} // Dummy entry

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)


	// 播种随机数生成器
	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) bool {

	if rf.killed() {
		return false
	}

	// paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {

		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}

	// 必须在加在这里否则加载前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对reply的返回状态进行分支
	switch reply.AppState {

	// 目标节点crash
	case AppKilled:
		{
			return false
		}

	// 目标节点正常返回
	case AppNormal:
		{
			// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			return true
		}

	//If AppendEntries RPC received from new leader: convert to follower(paper - 5.2)
	//reason: 出现网络分区，该Leader已经OutOfDate(过时）
	case AppOutOfDate:

		// 该节点变成追随者,并重置rf状态
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term

	}
	return ok
}

// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 节点crash
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = rf.currentTerm // Return current term even if killed
		reply.Success = false
		return
	}

	// 出现网络分区，args的任期，比当前raft的任期还小，说明args之前所在的分区已经OutOfDate
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	// 对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

