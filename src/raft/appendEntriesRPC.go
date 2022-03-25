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
	Term      int
	Success   bool
	NextIndex int
}

//处理收到的RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.NextIndex = 0
	if args.Term < rf.currentTerm {
		//Reply false if term < currentTerm
		DPrintf("[%v]--AE_Request--LeaderTermLittle--:To [%v],myTerm-%v,LeaderTerm-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.Success = false
	} else if args.PrevLogIndex < rf.log.getIndexIndexL(0) {
		DPrintf("[%v]--AE_Request--PrevLogIndexLittle--:To [%v],myTerm-%v,LeaderTerm-%v,PrevLogIndex-%v,myIndex0-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term, args.PrevLogIndex, rf.log.getIndexIndexL(0))
		reply.Success = false
		//可能是老消息也可能是Snapshot了,用来标记
		reply.NextIndex = args.PrevLogIndex + 1 + rf.log.getIndexIndexL(0)
	} else {
		if args.Term > rf.currentTerm && rf.role != Follower {
			DPrintf("[%v]--RoleChange--:get AE_RPC more Term from Leader-%v--", rf.me, args.LeaderId)
			rf.role = Follower
		}
		rf.currentTerm = args.Term
		//rf.resetElectionTimeL()

		//更新Log
		//DPrintf("[%v]getLastIndexL-%v,PrevLogIndex-%v,PrevLogTerm-%v,myEntries-%v,argEntries-%v", rf.me, rf.log.getLastIndexL(), args.PrevLogIndex, args.PrevLogTerm, rf.log, args.Entries)
		if rf.log.getLastIndexL() < args.PrevLogIndex || rf.log.Entries[args.PrevLogIndex-rf.log.getIndexIndexL(0)].Term != args.PrevLogTerm {
			//Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm
			if rf.log.getLastIndexL() < args.PrevLogIndex {
				reply.NextIndex = rf.log.getLastIndexL() + 1
				DPrintf("[%v]--AE_Request--conflict--LackEntry--:To [%v],myTerm-%v,LeaderTerm-%v,LastIndex-%v,PrevLogIndex-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.log.getLastIndexL(), args.PrevLogIndex)
			} else {
				reply.NextIndex = args.PrevLogIndex
				for reply.NextIndex-rf.log.getIndexIndexL(0)-1 >= 1 && rf.log.getIndexTermL(reply.NextIndex-rf.log.getIndexIndexL(0)-1) == rf.log.Entries[args.PrevLogIndex-rf.log.getIndexIndexL(0)].Term {
					reply.NextIndex--
				}
				DPrintf("[%v]--AE_Request--conflict--ConflictEntry--:To [%v],myTerm-%v,LeaderTerm-%v,Index-%v,myLogTerm-%v,PrevLogTerm-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term, args.PrevLogIndex, rf.log.Entries[args.PrevLogIndex].Term, args.PrevLogTerm)
			}

			reply.Success = false
		} else {
			//If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			//Append any new entries not already in the log
			//寻找冲突点
			i := args.PrevLogIndex - rf.log.getIndexIndexL(0) + 1
			j := 0
			for i < rf.log.getLen() && j < len(args.Entries) && rf.log.getIndexTermL(i) == args.Entries[j].Term {
				i++
				j++
			}
			if j >= len(args.Entries) {
				DPrintf("[%v]--AE_Request--Ignore--:To [%v],LastIndex-%v,LastTerm-%v", args.LeaderId, rf.me, rf.log.getLastIndexL(), rf.log.getLastTermL())
			} else {
				rf.log.Entries = append(rf.log.Entries[0:i], args.Entries[j:]...)
				DPrintf("[%v]--AE_Request--Success--:To [%v],myTerm-%v,LeaderTerm-%v,afterIndex-%v,LastIndex-%v,LastTerm-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.log.getIndexIndexL(i-1), rf.log.getLastIndexL(), rf.log.getLastTermL())

			}

			//更新commit
			if args.LeaderCommit > rf.commitIndex {
				//If leaderCommit > commitIndex, set commitIndex =
				//min(leaderCommit, index of last new entry)
				//更新commmit
				nxtCommitMax := Min(args.LeaderCommit, rf.log.getLastIndexL())
				DPrintf("[%v]--AE_Request--UpdateCommit--Success--:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
				rf.commitIndex = nxtCommitMax
			} else {
				//DPrintf("[%v]--AE_Request--UpdateCommit--No--:commitIndex-%v,LeaderCommit-%v", rf.me, rf.commitIndex, args.LeaderCommit)
			}
			reply.Success = true
		}
		rf.resetElectionTimeL()
	}
	//DPrintf("[%v]--request_Log--%v\n\n", rf.me, rf.log)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndexL() {
	//Leader更新commitIndex
	var tmp = make([]int, len(rf.peers))
	copy(tmp, rf.matchIndex)
	tmp[rf.me] = rf.log.getLastIndexL()
	sort.Ints(tmp)
	nxtCommitMax := tmp[len(rf.peers)/2]
	//DPrintf("matchIndex-%v,nxtCommitMax-%v,Log-%v", rf.matchIndex, nxtCommitMax, rf.log)
	if rf.log.getIndexTermL(nxtCommitMax-rf.log.getIndexIndexL(0)) == rf.currentTerm && nxtCommitMax >= rf.commitIndex {
		//更新commmit
		if rf.commitIndex == nxtCommitMax {
			DPrintf("[%v]--AE_True--UpdateCommit--Same:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
		} else {
			DPrintf("[%v]--AE_True--UpdateCommit--Success:commitIndex-%v,nxtCommitMax-%v", rf.me, rf.commitIndex, nxtCommitMax)
			rf.commitIndex = nxtCommitMax
		}
	} else {
		if nxtCommitMax < rf.commitIndex {
			DPrintf("[%v]--AE_True--UpdateCommit--CommitLittle--:commitIndex-%v,nxtCommitMax-%v,matchIndex-%v", rf.me, rf.commitIndex, nxtCommitMax, rf.matchIndex)
		} else {
			DPrintf("[%v]--AE_True--UpdateCommit--TermLittle--:commitIndex-%v,nxtCommitMax-%v,nxtCommitTerm-%v,cureentTerm-%v", rf.me, rf.commitIndex, nxtCommitMax, rf.log.Entries[nxtCommitMax-rf.log.getIndexIndexL(0)].Term, rf.currentTerm)
		}
	}
}

//发送快照
func (rf *Raft) dealSnapshotL(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.getIndexIndexL(0),
		LastIncludedTerm:  rf.log.getIndexTermL(0),
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	f := rf.sendInstallSnapshot(peer, &args, &reply)
	rf.mu.Lock()
	if f && rf.role == Leader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			DPrintf("[%v]--SS_False--TermLittle--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, peer, args.Term, reply.Term)
			if rf.role != Follower {
				DPrintf("[%v]--RoleChange--:get SS_Response more Term from [%v]--,myTerm_%v,replyTerm_%v", rf.me, peer, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.role = Follower
			}
			return
		}
		if args.LastIncludedIndex < rf.matchIndex[peer] {
			DPrintf("[%v]--SS_Response--ignore-:old response to [%v]", rf.me, peer)
		} else {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex

			rf.updateCommitIndexL()
		}
	}

}

//发送log
func (rf *Raft) dealReplicationL(peer int) {
	//初始化args
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]Entry, 0),
	}
	//需要发送的所有entry
	if rf.log.getLastEntryL().Index >= rf.nextIndex[peer] {
		args.Entries = make([]Entry, len(rf.log.Entries[rf.nextIndex[peer]-rf.log.getIndexIndexL(0):]))
		copy(args.Entries, rf.log.Entries[rf.nextIndex[peer]-rf.log.getIndexIndexL(0):])
	}
	args.PrevLogTerm, args.PrevLogIndex = rf.log.getIndexTermAndIndexL(rf.nextIndex[peer] - rf.log.getIndexIndexL(0) - 1)

	reply := AppendEntriesReply{}
	//DPrintf("[%v]--begin3 to [%v]--:term-%v", rf.me, peer, rf.currentTerm)
	rf.mu.Unlock()

	//DPrintf("[%v]--begin4 to [%v]--", rf.me, peer)
	//向server发送
	f := rf.sendAppendEntries(peer, &args, &reply)
	//DPrintf("[%v]--begin5 to [%v]--", rf.me, peer)
	rf.mu.Lock()
	if f && rf.role == Leader && rf.currentTerm == args.Term {
		//DPrintf("[%v]--begin6 to [%v]--:term-%v", rf.me, peer, rf.currentTerm)
		if reply.Success == false {
			if reply.Term <= args.Term {
				if reply.NextIndex >= args.PrevLogIndex+1 {
					//老消息
					if args.PrevLogIndex-rf.log.getIndexIndexL(0)+1 <= 0 || rf.nextIndex[peer] != rf.log.getIndexIndexL(args.PrevLogIndex-rf.log.getIndexIndexL(0)+1) {
						//如果是当前server有了新快照用新的,否则不更改
						tmp := rf.nextIndex[peer]
						rf.nextIndex[peer] = Max(rf.log.getIndexIndexL(0)+1, rf.nextIndex[peer])
						DPrintf("[%v]--AE_False--oldNext_%v to %v--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, tmp, rf.nextIndex[peer], peer, args.Term, reply.Term)
					} else {
						//Follower那边有了新快照,改为Follower快照点加1
						tmp := rf.nextIndex[peer]
						rf.nextIndex[peer] = reply.NextIndex - args.PrevLogIndex
						DPrintf("[%v]--AE_False--followerSnap_%v to %v--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, tmp, rf.nextIndex[peer], peer, args.Term, reply.Term)
					}
				} else {
					//更新nextIndex寻找最大共识
					rf.nextIndex[peer] = reply.NextIndex
					DPrintf("[%v]--AE_False--ReduceNext-%v--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, rf.nextIndex[peer], peer, args.Term, reply.Term)
				}
			} else {
				DPrintf("[%v]--AE_False--TermLittle--:fail append to [%v],myTerm_%v,replyTerm_%v", rf.me, peer, args.Term, reply.Term)
				if reply.Term > rf.currentTerm && rf.role != Follower {
					DPrintf("[%v]--RoleChange--:get AE_Response more Term from [%v]--,myTerm_%v,replyTerm_%v", rf.me, peer, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.role = Follower
				}
			}
			return
		}
		//更新matchIndex
		var nxtMatchIndex int
		if len(args.Entries) == 0 {
			nxtMatchIndex = args.PrevLogIndex
		} else {
			nxtMatchIndex = args.Entries[len(args.Entries)-1].Index
		}

		//DPrintf("[%v]peer-%v,nxtMatchIndex-%v,argsEntries-%v", rf.me, peer, nxtMatchIndex, args.Entries)
		if nxtMatchIndex == rf.matchIndex[peer] {
			DPrintf("[%v]--AE_Response--Ignore-:success append to [%v]", rf.me, peer)
		} else if nxtMatchIndex < rf.matchIndex[peer] {
			DPrintf("[%v]--AE_Response--Ignore-:old response to [%v]", rf.me, peer)
		} else {
			rf.matchIndex[peer] = nxtMatchIndex
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			//DPrintf("[%v]--AE_Response--UpdateMatch-%v--:success append to [%v],myTerm_%v,replyTerm_%v", rf.me, rf.matchIndex[peer], peer, rf.cureentTerm, reply.Term)
			rf.updateCommitIndexL()
		}
	}
	//DPrintf("[%v]--begin7 to [%v]--:term-%v", rf.me, peer, rf.currentTerm)

}

//向peer复制日志
func (rf *Raft) tryReplicationUL(peer int) {
	//DPrintf("[%v]--begin1 to [%v]--", rf.me, peer)
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	//DPrintf("[%v]--send_Log--%v\n", rf.me, rf.log)
	//DPrintf("[%v],peer-%v,nextIndex-%v,Index0-%v", rf.me, peer, rf.nextIndex[peer], rf.log.getIndexIndexL(0))
	//DPrintf("[%v]--begin2 to [%v]--:term-%v", rf.me, peer, rf.currentTerm)
	if rf.nextIndex[peer] <= rf.log.getIndexIndexL(0) {
		rf.dealSnapshotL(peer)
	} else {
		rf.dealReplicationL(peer)
	}
	rf.persist()
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
	rf.replicationCond[peer].L.Lock()
	for !rf.killed() {
		for !rf.needReplicationUL(peer) {
			rf.replicationCond[peer].Wait()
		}
		rf.tryReplicationUL(peer)
	}
	rf.replicationCond[peer].L.Unlock()
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
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.Entries[rf.lastApplied-rf.log.getIndexIndexL(0)].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()

				rf.applyCh <- applyMsg
				rf.mu.Lock()
				DPrintf("[%v]--CommitCommand--:index-%v,term-%v,command-%v", rf.me, rf.lastApplied, rf.log.getIndexTermL(rf.lastApplied-rf.log.getIndexIndexL(0)), applyMsg.Command)
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
	}
}
