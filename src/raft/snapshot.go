package raft

import (
	"6.824/labgob"
	"bytes"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//Follower处理RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%v]--InstallSnapshot_Request--LeaderTermLittle--:To [%v],myTerm-%v,LeaderTerm-%v", args.LeaderId, rf.me, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm && rf.role != Follower {
		DPrintf("[%v]--RoleChange--:get InstallSnapshot_RPC more Term from Leader-%v--", rf.me, args.LeaderId)
		rf.role = Follower
	}
	rf.currentTerm = args.Term
	rf.persist()

	rf.resetElectionTimeL()

	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("[%v]--InstallSnapshot fail--:commitIndex-%v lastIncludedTerm-%v lastIncludedIndex-%v", rf.me, rf.commitIndex, args.LastIncludedTerm, args.LastIncludedIndex)
		return
	}
	DPrintf("[%v]--InstallSnapshot success--:lastIncludedTerm-%v lastIncludedIndex-%v", rf.me, args.LastIncludedTerm, args.LastIncludedIndex)
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("[%v]--CondInstallSnapshot fail,commitIndex-%v lastIncludedTerm-%v lastIncludedIndex-%v", rf.me, rf.commitIndex, lastIncludedTerm, lastIncludedIndex)
		return false
	}
	if lastIncludedIndex >= rf.log.getLastIndexL() {
		rf.log.Entries = make([]Entry, 1)
	} else {
		rf.log.Entries = append(make([]Entry, 1), rf.log.Entries[lastIncludedIndex-rf.log.getIndexIndexL(0)+1:]...)
	}

	rf.log.setIndexTermAndIndexL(0, lastIncludedIndex, lastIncludedTerm)
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persistSnapshot(snapshot)

	DPrintf("[%v]--CondSnapshot receive--:snapLastTerm-%v,snapLastIndex-%v,myLastTerm-%v,myLastIndex-%v", rf.me, rf.log.getIndexTermL(0), rf.log.getIndexIndexL(0), rf.log.getLastTermL(), rf.log.getLastIndexL())

	DPrintf("[%v]--CondInstall_Log--%v", rf.me, rf.log)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.log.getIndexIndexL(0)
	if index <= firstIndex {
		DPrintf("[%v]--Snapshot fail--:index-%v less ,snapLastTerm-%v,snapLastIndex-%v,myLastTerm-%v,myLastIndex-%v", rf.me, index, rf.log.getIndexTermL(0), rf.log.getIndexIndexL(0), rf.log.getLastTermL(), rf.log.getLastIndexL())
		return
	}
	index -= firstIndex
	rf.log.setIndexTermAndIndexL(0, rf.log.getIndexIndexL(index), rf.log.getIndexTermL(index))
	rf.log.Entries = append(rf.log.Entries[:1], rf.log.Entries[index+1:]...)

	rf.persistSnapshot(snapshot)
	DPrintf("[%v]--Snapshot_Log--%v", rf.me, rf.log)
	DPrintf("[%v]--Snapshot receive--:index-%v,snapLastTerm-%v,snapLastIndex-%v,myLastTerm-%v,myLastIndex-%v", rf.me, index, rf.log.getIndexTermL(0), rf.log.getIndexIndexL(0), rf.log.getLastTermL(), rf.log.getLastIndexL())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedFor)
	//e.Encode(rf.log)
	//data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
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
	e.Encode(rf.log)
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
	var log Log
	if err := d.Decode(&currentTerm); err != nil {
		DPrintf("[%v][read currentTerm error]-%v", rf.me, err)
	} else if err := d.Decode(&votedFor); err != nil {
		DPrintf("[%v][read votedFor error]-%v", rf.me, err)
	} else if err := d.Decode(&log); err != nil {
		DPrintf("[%v][read log error]-%v", rf.me, err)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}
