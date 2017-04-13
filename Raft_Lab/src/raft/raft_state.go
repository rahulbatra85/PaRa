//Name: raft.go
//Description: This file defines API to get/set raft persistent state
//Author: Rahul Batra

package raft

func (rf *Raft) getCurrentTerm() int {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return rf.CurrentTerm
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	rf.CurrentTerm = term
	//rf.persist()
}

func (rf *Raft) getVotedFor() int {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return rf.VotedFor
}

func (rf *Raft) setVotedFor(id int) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	rf.VotedFor = id
	//rf.persist()
}

func (rf *Raft) getState() RaftState {
	rf.stmu.RLock()
	defer rf.stmu.RUnlock()
	return rf.state
}

func (rf *Raft) setState(st RaftState) {
	rf.stmu.Lock()
	defer rf.stmu.Unlock()
	rf.state = st
}
