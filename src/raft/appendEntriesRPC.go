package raft

import (
	"sort"
	"time"
)

type AppendEntriesType int

const (
	HeartBeat   AppendEntriesType = 1
	Replication AppendEntriesType = 2
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//处理收到的RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.cureentTerm
	if args.Term < rf.cureentTerm {
		//Reply false if term < currentTerm
		DPrintf("[%v]--AE_Request--LeaderTermLittle--:To [%v],myTerm-%v,LeaderTerm-%v", args.LeaderId, rf.me, rf.cureentTerm, args.Term)
		reply.Success = false
	} else {
		if args.Term > rf.cureentTerm && rf.role != Follower {
			DPrintf("[%v]--RoleChange--:get AE_RPC more Term from Leader-%v--", rf.me, args.LeaderId)
			rf.role = Follower
		}
		rf.cureentTerm = args.Term
		rf.resetElectionTimeL()

		//更新Log
		if rf.log.getLastIndexL() < args.PrevLogIndex || rf.log.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
			//Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm
			if rf.log.getLastIndexL() < args.PrevLogIndex {
				DPrintf("[%v]--AE_Request--conflict--LackEntry--:To [%v],myTerm-%v,LeaderTerm-%v,LastIndex-%v,PrevLogIndex-%v", args.LeaderId, rf.me, rf.cureentTerm, args.Term, rf.log.getLastIndexL(), args.PrevLogIndex)
			} else {
				DPrintf("[%v]--AE_Request--conflict--ConflictEntry--:To [%v],myTerm-%v,LeaderTerm-%v,Index-%v,myLogTerm-%v,PrevLogTerm-%v", args.LeaderId, rf.me, rf.cureentTerm, args.Term, args.PrevLogIndex, rf.log.Entries[args.PrevLogIndex].Term, args.PrevLogTerm)
			}

			reply.Success = false
		} else {
			//If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			//Append any new entries not already in the log
			//寻找冲突点
			i := args.PrevLogIndex + 1
			j := 0
			for i < rf.log.getLen() && j < len(args.Entries) && rf.log.getIndexTermL(i) == args.Entries[j].Term {
				i++
				j++
			}
			if j >= len(args.Entries) {
				DPrintf("[%v]--AE_Request--Ignore--:To [%v],LastIndex-%v,LastTerm-%v", args.LeaderId, rf.me, rf.log.getLastIndexL(), rf.log.getLastTermL())
			} else {
				rf.log.Entries = append(rf.log.Entries[0:i], args.Entries[j:]...)
				DPrintf("[%v]--AE_Request--Success--:To [%v],myTerm-%v,LeaderTerm-%v,afterIndex-%v,LastIndex-%v,LastTerm-%v", args.LeaderId, rf.me, rf.cureentTerm, args.Term, i-1, rf.log.getLastIndexL(), rf.log.getLastTermL())

			}

			//更新commit
			if args.LeaderCommit > rf.commitIndex {
				//If leaderCommit > commitIndex, set commitIndex =
				//min(leaderCommit, index of last new entry)
				//更新commmit
				nxtCommitMax := Min(args.LeaderCommit, rf.log.getLastIndexL())
				DPrintf("[%v]--AE_Request--UpdateCommit--Success--:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
				rf.commitIndex = nxtCommitMax
			}
			reply.Success = true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//向peer复制日志
func (rf *Raft) tryReplicationUL(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	//初始化args
	args := AppendEntriesArgs{
		Term:         rf.cureentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]Entry, 0),
	}
	//需要发送的所有entry
	if rf.log.getLastEntryL().Index >= rf.nextIndex[peer] {
		args.Entries = rf.log.Entries[rf.nextIndex[peer]:]
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.log.getIndexTermAndIndexL(rf.nextIndex[peer] - 1)

	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	//向server发送
	f := rf.sendAppendEntries(peer, &args, &reply)
	rf.mu.Lock()
	if f && rf.role == Leader && rf.cureentTerm == args.Term {
		if reply.Success == false {
			if reply.Term <= args.Term {
				//更新nextIndex寻找最大共识
				rf.nextIndex[peer]--
				DPrintf("[%v]--AE_False--ReduceNext-%v--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, rf.nextIndex[peer], peer, args.Term, reply.Term)
			} else {
				DPrintf("[%v]--AE_False--TermLittle--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, peer, args.Term, reply.Term)
				if reply.Term > rf.cureentTerm && rf.role != Follower {
					DPrintf("[%v]--RoleChange--:get AE_Response more Term from [%v]--,myTerm_%v,replyTerm_%v", rf.me, peer, rf.cureentTerm, reply.Term)
					rf.cureentTerm = reply.Term
					rf.role = Follower
				}

			}
		} else if len(args.Entries) == 0 {
			DPrintf("[%v]--AE_Response--Ignore-:success append to [%v]", rf.me, peer)
		} else {
			rf.matchIndex[peer] = args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			//DPrintf("[%v]--AE_Response--UpdateMatch-%v--:success append to [%v],myTerm_%v,replyTerm_%v", rf.me, rf.matchIndex[peer], peer, rf.cureentTerm, reply.Term)

			//更新commit
			var tmp = make([]int, len(rf.peers))
			copy(tmp, rf.matchIndex)
			tmp[rf.me] = rf.log.getLen()
			sort.Ints(tmp)
			nxtCommitMax := tmp[len(rf.peers)/2]
			if nxtCommitMax < rf.commitIndex {
				DPrintf("[%v]--AE_True--UpdateCommit--Error--:commitIndex-%v,nxtCommitMax-%v,matchIndex-%v", rf.me, rf.commitIndex, nxtCommitMax, rf.matchIndex)
			} else if rf.log.Entries[nxtCommitMax].Term == rf.cureentTerm {
				//更新commmit
				if rf.commitIndex == nxtCommitMax {
					DPrintf("[%v]--AE_True--UpdateCommit--Same:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
				} else {
					DPrintf("[%v]--AE_True--UpdateCommit--Success:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
					rf.commitIndex = nxtCommitMax
				}
			} else {
				DPrintf("[%v]--AE_True--UpdateCommit--TermLittle--:commitIndex-%v,nxtCommitMax-%v,nxtCommitTerm-%v,cureentTerm-%v", rf.me, rf.commitIndex, nxtCommitMax, rf.log.Entries[nxtCommitMax].Term, rf.cureentTerm)
			}
		}

	}
	rf.mu.Unlock()
}

//判断是否需要进行日志复制
func (rf *Raft) needReplicationUL(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader && rf.log.getLastEntryL().Index >= rf.nextIndex[peer] {
		return true
	}
	return false
}

//按顺序处理新增加Entry,防止一次流量过大造成重复的Entry发送浪费资源
func (rf *Raft) replicationQueue(peer int) {
	for !rf.killed() {
		rf.replicationCond[peer].L.Lock()
		for !rf.needReplicationUL(peer) {
			rf.replicationCond[peer].Wait()
		}
		rf.replicationCond[peer].L.Unlock()
		rf.tryReplicationUL(peer)
	}
}

//进行心跳发送或者激活Replication
func (rf *Raft) doAppendEntryUL(AEType AppendEntriesType) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if AEType == HeartBeat {
			go rf.tryReplicationUL(i)
		} else {
			rf.replicationCond[i].Signal()
		}
	}
}

//将commit的entry应用到状态机
func (rf *Raft) commitToRSM() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.mu.Unlock()
			rf.lastApplied++

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Entries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			DPrintf("[%v]--CommitCommand--:index-%v,term-%v", rf.me, rf.lastApplied, rf.log.getIndexTermL(rf.lastApplied))
			time.Sleep(20 * time.Millisecond)
		} else {
			rf.mu.Unlock()
		}
	}
}
