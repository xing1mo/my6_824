package raft

type AppendEntriesType int

const (
	HeartBeat   AppendEntriesType = 1
	NOOP        AppendEntriesType = 2
	Replication AppendEntriesType = 3
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log

	LeaderCommit int

	AEType AppendEntriesType
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v]--AE_Request--:Leader try to append %v,LeaderTerm-%v,myTerm-%v", args.LeaderId, rf.me, args.Term, rf.cureentTerm)

	if args.Term < rf.cureentTerm {
		//Reply false if term < currentTerm
		reply.Success = false
	} else if false {
		//Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm

	} else if false {
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it
	} else if false {
		//Append any new entries not already in the log
	} else if false {
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
	} else {
		if args.Term > rf.cureentTerm && rf.role != Follower {
			DPrintf("[%v]--RoleChange--:change to Follower because --get AE_RPC more Term from Leader-%v--", rf.me, args.LeaderId)
			rf.role = Follower
		}
		rf.cureentTerm = args.Term
		rf.resetElectionTimeL()
		reply.Success = true
	}
	reply.Term = rf.cureentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//发送一个entry,可以是各种类型
func (rf *Raft) doAppendEntryL(aeType AppendEntriesType) {
	//初始化args
	args := AppendEntriesArgs{
		Term:         rf.cureentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		AEType:       aeType,
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.log.getLastTermAndIndexL()

	if aeType == NOOP {
		//添加一个空Log
		args.Entries = make([]Log, 0)
		args.Entries = append(args.Entries, Log{})
	} else if aeType == Replication {
		//
	}

	rf.mu.Unlock()
	//向所有server发送
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(idx int) {
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(idx, &args, &reply) {
					rf.mu.Lock()
					if reply.Success == false {
						//
						DPrintf("[%v]--AE_Response--:fail append to %v,myTerm_%v,replyTerm_%v", rf.me, idx, rf.cureentTerm, reply.Term)
					} else {
						DPrintf("[%v]--AE_Response--:success append to %v,myTerm_%v,replyTerm_%v", rf.me, idx, rf.cureentTerm, reply.Term)
					}
					if reply.Term > rf.cureentTerm && rf.role != Follower {
						DPrintf("[%v]--RoleChange--:change to Follower because --get AE_Response more Term from %v--", rf.me, idx)
						rf.cureentTerm = reply.Term
						rf.role = Follower
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
	rf.mu.Lock()
}
