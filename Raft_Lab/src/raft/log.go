//Name: log.go
//Description: Contains code pertaining to raft log
//Author: Rahul Batra

package raft

//
//Raft Log Entry
//
type LogEntry struct {
	Term         int
	Cmd          interface{}
	ClientId     int
	ClientSeqNum int
}

//
//Returns last log index
//
func (rf *Raft) GetLastLogIndex() int {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return len(rf.Log) - 1
}

//
//Returns Last Log Entry Term
//
func (rf *Raft) GetLastLogTerm() int {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return rf.Log[len(rf.Log)-1].Term
}

//
//Appends entry to log
//
func (rf *Raft) AppendLog(entry LogEntry) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	rf.Log = append(rf.Log, entry)
	//rf.persist()
	DPrintf("Serv[%d]: AppendLog=%v", rf.me, rf.Log)
}

//
//Thread-Safe way for a server to append entry to log in
//response to a client request
//
func (rf *Raft) AppendLogLeader(command interface{}) (int, int, bool) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if rf.state == LEADER {
		isLeader = true
		term = rf.CurrentTerm
		index = len(rf.Log)
		entry := LogEntry{Term: term, Cmd: command}
		rf.Log = append(rf.Log, entry)
		//rf.persist()
		DPrintf("Serv[%d], AppendLogLeader Idx=%d, Term=%d. Exit\n", rf.me, index, term)
	} else {
		DPrintf("Serv[%d], AppendLogLeader NOT Leader\n", rf.me)
	}

	return index, term, isLeader
}

//Return Log Entry at specified index
func (rf *Raft) GetLogEntry(idx int) LogEntry {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return rf.Log[idx]
}

//
//Removes all entries starting from index "idx"
//
func (rf *Raft) RemoveLogEntry(idx int) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	if len(rf.Log) > idx {
		rf.Log = rf.Log[:idx]
	}
	//rf.persist()
	DPrintf("Serv[%d]: RemoveLog at %d. Log=%v\n", rf.me, idx, rf.Log)
}
