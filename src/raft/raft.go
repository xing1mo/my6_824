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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	//标识该命令生成时的term,若后续的Term发生变化,表示重新选举了,旧的命令不执行
	//server失去leader地位或者Term改变后，抛弃apply结果，因为可能原index被覆盖了，导致出现错误结果
	CommandTerm int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Leader    Role = 1
	Candidate Role = 2
	Follower  Role = 3
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//目前扮演的角色
	role Role
	//选举超时
	electionTimeout time.Time
	//心跳时间
	heartBeatTime time.Duration

	//向应用回复已经commit的log
	applyCh chan ApplyMsg
	//处理新增加Entry,防止一次流量过大造成重复的Entry发送浪费资源
	replicationCond []*sync.Cond

	//Persistent state on all servers:
	currentTerm int //init 1
	votedFor    int //init -1,表示没投票
	log         Log

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

//init all server
func (rf *Raft) initServerUL() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = Log{make([]Entry, 0)}
	rf.log.Entries = append(rf.log.Entries, Entry{Term: 0, Index: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = Follower
	rf.heartBeatTime = time.Duration(50)

	//为每个节点启一个线程处理replication
	rf.replicationCond = make([]*sync.Cond, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.replicationCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicationQueue(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	//快照了一定已经commit了
	rf.lastApplied = rf.log.getIndexIndexL(0)
	rf.commitIndex = rf.log.getIndexIndexL(0)

	//处理Entry应用到状态机
	go rf.commitToRSM()

	rf.resetElectionTimeL()

	DPrintf("[%v]--init--:Server--Term-%v-lastApplied-%v-commitIndex-%v", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
}

//init leader
func (rf *Raft) initLeaderL() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		_, rf.nextIndex[i] = rf.log.getLastTermAndIndexL()
		rf.nextIndex[i]++
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	rf.role = Leader
	DPrintf("[%v]--init--:Leader--Term-%v", rf.me, rf.currentTerm)
}

//init candidate
func (rf *Raft) initCandidateL() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	rf.persist()

	DPrintf("[%v]--init--:Candidate--Term-%v", rf.me, rf.currentTerm)
}

func (rf *Raft) resetElectionTimeL() {
	tmp := rand.Int()%150 + 150
	rf.electionTimeout = time.Now().Add(time.Duration(tmp) * time.Millisecond)
	//DPrintf("[%v]--resetElectionTimeL--:add-%v,electionTimeout-%v", rf.me, tmp, rf.electionTimeout)
}

//超时选举
func (rf *Raft) election() {
	for rf.killed() == false {
		rf.mu.Lock()
		if time.Now().After(rf.electionTimeout) {
			//DPrintf("[%v]--timeout--:Now-%v,electionTimeout%v", rf.me, time.Now(), rf.electionTimeout)
			rf.resetElectionTimeL()
			rf.initCandidateL()
			arg := rf.initElectionArgsL()
			go rf.doElectionUL(arg)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(2) * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peser hasn't received
// heartsbeats recently.
//心跳
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.role == Leader {
			rf.resetElectionTimeL()
			DPrintf("[%v]--doHeartBeat--:begin to send HeartBeat-%v", rf.me, rf.currentTerm)
			//DPrintf("[%v]--send_Log--%v\n\n", rf.me, rf.log)
			rf.mu.Unlock()
			rf.doAppendEntryUL(HeartBeat)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(rf.heartBeatTime * time.Millisecond)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
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
	// Your code here (2B).
	rf.mu.Lock()
	index := rf.log.getLastIndexL() + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		DPrintf("[%v]--AcceptCommand--:new entry at Index-%v Term-%v,Command-%v", rf.me, index, term, command)
		rf.log.appendEntryL(Entry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		rf.mu.Unlock()
		rf.doAppendEntryUL(Replication)
	} else {
		rf.mu.Unlock()
	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.initServerUL()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.election()
	return rf
}
