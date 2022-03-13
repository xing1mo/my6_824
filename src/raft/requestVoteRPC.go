package raft

import "sync"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("--RV_Request--:Candidate-%v try to get from %v", args.CandidateId, rf.me)
	//获得最后log
	lastLog := rf.log.getLast()
	var lastLogIndex, lastLogTerm int
	if lastLog == nil {
		lastLogIndex = 0
		lastLogTerm = 0
	} else {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	//判断是否需要投票
	if args.Term > rf.cureentTerm {
		rf.cureentTerm = args.Term
		rf.votedFor = -1
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.cureentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.cureentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//其中有对rf的锁
func (rf *Raft) doElection() {
	rf.mu.Lock()

	rf.role = Candidate
	rf.cureentTerm++
	rf.votedFor = rf.me

	//参数初始化
	args := RequestVoteArgs{
		Term:        rf.cureentTerm,
		CandidateId: rf.me,
	}
	lastLog := rf.log.getLast()
	if lastLog == nil {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = lastLog.Index
		args.LastLogTerm = lastLog.Term
	}

	//记录票数
	voteCnt := 1
	//记录完成数
	finishCnt := 1
	//condition通知多少选票
	cond := sync.NewCond(&sync.Mutex{})

	rf.mu.Unlock()
	//发送每个选票请求
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(idx int) {
				reply := RequestVoteReply{}
				f := rf.sendRequestVote(idx, &args, &reply)
				cond.L.Lock()
				if f {
					rf.mu.Lock()
					if reply.VoteGranted {
						voteCnt++
						DPrintf("--RV_Response--:%v getVote from %v", rf.me, idx)
					} else {
						DPrintf("--RV_Response--:%v rejectedVote from %v", rf.me, idx)
					}

					rf.mu.Unlock()
				} else {
					DPrintf("--RV_Response--:%v can't receive response from %v", rf.me, idx)
				}
				finishCnt++
				cond.L.Unlock()
				//DPrintf("--finish--:%v finish vote", rf.me)
				cond.Broadcast()
			}(i)
		}
	}

	//等待选票数够或者所有机器都完成了
	DPrintf("--wait--:%v wait vote,vote-%v,finish-%v", rf.me, voteCnt, finishCnt)
	cond.L.Lock()
	for voteCnt < len(rf.peers)/2+1 && finishCnt < len(rf.peers) {
		cond.Wait()
		DPrintf("--wait--:%v wait vote,vote-%v,finish-%v", rf.me, voteCnt, finishCnt)
	}
	cond.L.Unlock()

	rf.mu.Lock()
	DPrintf("--wait--:%v wait vote,vote-%v,finish-%v", rf.me, voteCnt, finishCnt)
	rf.mu.Unlock()
	if voteCnt >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	} else {
		rf.mu.Lock()
		DPrintf("--RoleChange--:%v change to Follower because --Vote Failed", rf.me)
		rf.role = Follower
		rf.mu.Unlock()
	}
}

//晋升leader,其中有对rf的锁
func (rf *Raft) becomeLeader() {
	rf.initLeader()
	rf.doAppendEntry(NOOP)
}
