package raft

//This file defines API to get/set raft persistent state

func (r *RaftNode) getCurrentTerm() int {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return r.CurrentTerm
}

func (r *RaftNode) setCurrentTerm(term int) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	r.CurrentTerm = term
	//persist //\todo
}

func (r *RaftNode) getVotedFor() string {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return r.VotedFor
}

func (r *RaftNode) setVotedFor(id string) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	r.VotedFor = id
	//persist //\todo
}

func (r *RaftNode) getState() RaftState {
	r.stmu.RLock()
	defer r.stmu.RUnlock()
	return r.state
}

func (r *RaftNode) setState(st RaftState) {
	r.stmu.Lock()
	defer r.stmu.Unlock()
	r.state = st
}
