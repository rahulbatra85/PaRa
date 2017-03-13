package raft

//
//Contains code pertaining to raft log
//

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
func (r *RaftNode) GetLastLogIndex() int {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return len(r.Log) - 1
}

//
//Returns Last Log Entry Term
//
func (r *RaftNode) GetLastLogTerm() int {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return r.Log[len(r.Log)-1].Term
}

//
//Appends entry to log
//
func (r *RaftNode) AppendLog(entry LogEntry) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	r.Log = append(r.Log, entry)
	//persist \\todo
}

/*
//
//Thread-Safe way for a server to append entry to log in
//response to a client request
//
func (r *RaftNode) AppendLogLeader(command interface{}) (int, int, bool) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if r.state == LEADER {
		isLeader = true
		term = r.CurrentTerm
		index = len(r.Log)
		entry := LogEntry{Term: term, Cmd: command}
		r.Log = append(r.Log, entry)
		//persist \\todo
	}

	return index, term, isLeader
}*/

//Return Log Entry at specified index
func (r *RaftNode) GetLogEntry(idx int) LogEntry {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return r.Log[idx]
}

//
//Removes all entries starting from index "idx"
//
func (r *RaftNode) RemoveLogEntry(idx int) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	if len(r.Log)-1 > idx {
		r.Log = r.Log[:idx]
	}
	//persist \\todo
}
