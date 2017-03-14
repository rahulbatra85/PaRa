package raft

import (
	"math/rand"
	"sort"
	"time"
)

type ElectionResultMsg struct {
	result bool
	term   int
}

//
//Main raft_server go routine
//
func (r *RaftNode) run_server() {
	r.INF(" Server Starting\n")

	//Kick of another go routine to apply any commited entries in the log
	go r.UpdateSM()

	//Main raft_server loop
	for {
		if r.nextState == FOLLOWER {
			r.setState(FOLLOWER)
			r.nextState = r.do_follower()
			r.INF(" Follower State Exit\n")
		} else if r.nextState == CANDIDATE {
			r.setState(CANDIDATE)
			r.nextState = r.do_candidate()
			r.INF(" Candidate State Exit\n")
		} else if r.nextState == LEADER {
			r.setState(LEADER)
			r.nextState = r.do_leader()
			r.INF(" Leader State Exit\n")
		} else {
			//fatal error
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in FOLLOWER state
//
func (r *RaftNode) do_follower() (nextState RaftState) {
	r.INF(" Follower State Enter Term=%d\n", r.getCurrentTerm())
	rcvAppendEntries := false
	for {
		select {
		case msg := <-r.appendEntriesMsgCh:
			r.DBG(" Rcv on AppendEntriesMsgCh\n")
			fallback := r.handleAppendEntries(msg)
			if fallback != true {
				r.INF(" From Leader\n")
				rcvAppendEntries = true
			}
		case msg := <-r.requestVoteMsgCh:
			r.INF(" Rcv on RequestVoteMsgCh\n")
			r.handleRequestVote(msg)
		case <-r.appendEntriesReplyCh:
			//Do nothing
		case <-r.requestVoteReplyCh:
			//Do nothing
		case <-r.makeElectionTimeout():
			if rcvAppendEntries == true {
				r.INF(" Rcv on ElecTO\n")
				rcvAppendEntries = false
			} else {
				return CANDIDATE
			}
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in CANDIDATE state
//
func (r *RaftNode) do_candidate() (nextState RaftState) {
	r.INF(" Candidate State Term=%d\n", r.getCurrentTerm())
	r.setCurrentTerm(r.getCurrentTerm() + 1)
	r.setVotedFor(r.Id)
	voteCnt := 1
	r.requestVotes()
	r.DBG(" Wait in Candidate State\n")
	for {
		select {
		case msg := <-r.appendEntriesMsgCh:
			r.DBG(" Rcv on AppendEntriesMsgCh\n")
			r.handleAppendEntries(msg)
		case msg := <-r.requestVoteMsgCh:
			r.DBG(" Rcv on RequestVoteMsgCh\n")
			if r.handleCandidateOrLeaderRequestVote(msg) == true {
				return FOLLOWER
			}
		case <-r.appendEntriesReplyCh:
			//Do nothing
		case msg := <-r.requestVoteReplyCh:
			if msg.Term > r.getCurrentTerm() {
				r.setCurrentTerm(msg.Term)
				return FOLLOWER
			}
			if msg.VoteGranted == true {
				voteCnt++
				r.INF(" Vote Resp, cnt=%d\n", voteCnt)
				if voteCnt >= r.config.Majority {
					return LEADER
				}
			}
		case <-r.makeElectionTimeout():
			r.DBG(" Rcv on ElecTO\n")
			return CANDIDATE
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in LEADER state
//
func (r *RaftNode) do_leader() (nextState RaftState) {
	r.INF(" Leader State Term=%d\n", r.getCurrentTerm())
	r.matchIndex[r.localAddr] = 0
	r.nextIndex[r.localAddr] = r.GetLastLogIndex() + 1
	for _, n := range r.othersAddr {
		if n != r.localAddr {
			r.nextIndex[n] = r.GetLastLogIndex() + 1
			r.matchIndex[n] = r.GetLastLogIndex()
		}
	}
	notMajorityCnt := 0
	for {
		select {
		case msg := <-r.appendEntriesMsgCh:
			if r.handleAppendEntries(msg) == true {
				return FOLLOWER
			}
		case msg := <-r.requestVoteMsgCh:
			if r.handleCandidateOrLeaderRequestVote(msg) == true {
				return FOLLOWER
			}
		case <-r.appendEntriesReplyCh:
			//\TODO
		case <-r.requestVoteReplyCh:
			//Do nothing
		case <-r.makeHeartbeatTimeout():
			fallBack, sentToMajority := r.sendHeartBeats()
			if fallBack == true {
				return FOLLOWER
			}

			//If HeartBeat was sent to a majority in timeout period,
			//then everything is ok and continue to be leader.
			//However, if leader is partiioned and heartbeats weren't sent successfully
			//to majorty for heartbeat period, then step-down
			if sentToMajority == true {
				notMajorityCnt = 0
			} else {
				notMajorityCnt++
			}
			if notMajorityCnt == 2 {
				return FOLLOWER
			}
		}

		//Update Commit Index
		r.UpdateCommitIdx()
	}
}

//
//In separate go routine for each peer, it sends RequestVote RPC to a peer,
//waits for the response and then forwards it to the server main-loop
//
func (r *RaftNode) requestVotes() {
	r.DBG(" Starting Election\n")
	args := RequestVoteArgs{}
	args.Term = r.getCurrentTerm()
	args.CandidateId = r.Id
	args.LastLogIdx = r.GetLastLogIndex()
	args.LastLogTerm = r.GetLogEntry(args.LastLogIdx).Term
	for _, n := range r.othersAddr {
		if n != r.localAddr {
			go func(n NodeAddr) {
				reply := &RequestVoteReply{}
				err := r.RequestVoteRPC(&n, args, reply)
				if err == nil {
					r.requestVoteReplyCh <- *reply
				}
			}(n)
		}
	}
	return
}

//
//This function is called by the leader to sendHeartbeats.
//If a follower is up to date, then it simply send appendEntries
//with no entries(empty). Otherwise, it sends the follower the
//newer entries
func (r *RaftNode) sendHeartBeats() (fallBack, sentToMajority bool) {
	r.DBG(" Send Heartbeats Enter\n")

	term := r.getCurrentTerm()
	successCnt := 1
	r.matchIndex[r.localAddr] = r.GetLastLogIndex()

	for _, n := range r.othersAddr {
		if n != r.localAddr {
			req := AppendEntriesArgs{}
			req.Term = term
			req.LeaderId = r.Id
			reply := &AppendEntriesReply{}
			req.PrevLogIdx = r.nextIndex[n] - 1
			req.PrevLogTerm = r.GetLogEntry(r.nextIndex[n] - 1).Term
			if r.GetLastLogIndex() >= r.nextIndex[n] {
				r.DBG(" AppendEntries[%d] PrevLogIdx:%d\n", n, req.PrevLogIdx)
				req.Entries = make([]LogEntry, r.GetLastLogIndex()-r.nextIndex[n]+1)
				for i, j := r.nextIndex[n], 0; i <= r.GetLastLogIndex(); i, j = i+1, j+1 {
					req.Entries[j] = r.GetLogEntry(i)
				}
			} else {
				req.Entries = nil
			}
			req.LeaderCommit = r.commitIndex

			err := r.AppendEntriesRPC(&n, req, reply)
			if err == nil {
				if reply.Term > r.getCurrentTerm() {
					r.setCurrentTerm(reply.Term)
					fallBack = true
					sentToMajority = false
					r.DBG(" Send Heartbeats Exit %v %v\n", fallBack, sentToMajority)
					return fallBack, sentToMajority
				}
				if reply.Success == true {
					successCnt++
					r.nextIndex[n] = req.PrevLogIdx + len(req.Entries) + 1
					r.matchIndex[n] = req.PrevLogIdx + len(req.Entries)
				} else {
					if r.nextIndex[n] > 1 {
						r.nextIndex[n]--
					}
				}
			}
		}
	}
	if successCnt >= r.config.Majority {
		fallBack = false
		sentToMajority = true
	} else {
		fallBack = false
		sentToMajority = false
	}

	r.DBG(" Send Heartbeats Exit %v %v\n", fallBack, sentToMajority)
	return fallBack, sentToMajority
}

// This function creates timer channel with random timeout.
func (r *RaftNode) makeElectionTimeout() <-chan time.Time {
	val := rand.Int()
	val = val + 100*rand.Int()
	if val < 0 {
		val *= -1
	}
	to := (val % r.config.ElectionTimeout) + r.config.ElectionTimeout
	r.INF("ElectionTimeout=%d\n", to)
	return time.After(time.Duration(to) * time.Millisecond)
}

//This functions creates timer channel with heartBeat timeout
func (r *RaftNode) makeHeartbeatTimeout() <-chan time.Time {
	return time.After(time.Duration(r.config.HeartbeatFrequency) * time.Millisecond)
}

//Period for UpdateSM go routine to be woken up
func (r *RaftNode) makeUpdateSMPeriod() <-chan time.Time {
	return time.After(time.Duration(10) * time.Millisecond)
}

//
//This function handles AppendEntries RPC as per the description in raft paper
//
func (r *RaftNode) handleAppendEntries(msg AppendEntriesMsg) bool {
	retVal := false
	reply := AppendEntriesReply{}
	r.DBG(" handleAppendEntries Enter\n")
	r.DBG(" 	Term=%d, NumEnt=%d, LeadId=%d, PrevLogIdx=%d,PrevLogTerm=%d,LeadCom=%d\n", msg.args.Term, len(msg.args.Entries),
		msg.args.LeaderId, msg.args.PrevLogIdx, msg.args.PrevLogTerm, msg.args.LeaderCommit)

	//Update our term if greater than the current term
	if msg.args.Term > r.getCurrentTerm() {
		r.setCurrentTerm(msg.args.Term)
		if r.state != FOLLOWER {
			retVal = true
		}
	}

	//If AppendEntriesRequest term is smaller
	if msg.args.Term < r.getCurrentTerm() {
		//Send Reply on the channel
		reply.Term = r.getCurrentTerm()
		reply.Success = false
		msg.reply <- reply
		r.DBG(" handleAppendEntries Exit, reply=%v\n", reply)
		return retVal
	}

	//If PrevLogTerm doesn't match with term of entry at PrevLogIndex
	if msg.args.PrevLogIdx > r.GetLastLogIndex() ||
		msg.args.PrevLogTerm != r.GetLogEntry(msg.args.PrevLogIdx).Term {
		//Send Reply on the channel
		reply.Term = r.getCurrentTerm()
		reply.Success = false
		msg.reply <- reply
		r.DBG(" handleAppendEntries Exit, reply=%v\n", reply)
		return retVal
	}

	//Replace conflicting entries and append new entries
	i := msg.args.PrevLogIdx
	ei := 0
	for ei = 0; i < r.GetLastLogIndex() && ei < len(msg.args.Entries); ei++ {
		i++
		if r.GetLogEntry(i).Term != msg.args.Entries[ei].Term {
			break
		}
	}
	r.RemoveLogEntry(i)
	for ; ei < len(msg.args.Entries); ei++ {
		i++
		r.DBG(" Appended@%d\n", i)
		r.AppendLog(msg.args.Entries[ei])
		//r.log[i] = msg.args.Entries[ei]
	}

	//Update commit index
	if msg.args.LeaderCommit > r.commitIndex {
		if msg.args.LeaderCommit > (i - 1) {
			r.commitIndex = msg.args.LeaderCommit
		} else if msg.args.LeaderCommit < (i - 1) {
			r.commitIndex = i - 1
		}
		r.DBG(" Updt CommitIdx: %d\n", r.commitIndex)
	}

	//Send Reply on the channel
	reply.Term = r.getCurrentTerm()
	reply.Success = true
	msg.reply <- reply
	r.DBG(" handleAppendEntries Exit, reply=%v\n", reply)
	return retVal
}

//
//handleRequestVote
//
func (r *RaftNode) handleRequestVote(msg RequestVoteMsg) {
	r.DBG(" Handler Request Vote Enter\n")
	reply := RequestVoteReply{}

	if r.getCurrentTerm() > msg.args.Term {
		reply.VoteGranted = false
	} else {
		if r.getCurrentTerm() < msg.args.Term {
			r.setCurrentTerm(msg.args.Term)
			r.setVotedFor("")
		}
		if r.getVotedFor() == msg.args.CandidateId || r.getVotedFor() == "" {
			if r.GetLastLogTerm() > msg.args.LastLogTerm {
				reply.VoteGranted = false
			} else if r.GetLastLogTerm() < msg.args.LastLogTerm {
				reply.VoteGranted = true
				r.setVotedFor(msg.args.CandidateId)
			} else {
				if r.GetLastLogIndex() > msg.args.LastLogIdx {
					reply.VoteGranted = false
				} else {
					reply.VoteGranted = true
					r.setVotedFor(msg.args.CandidateId)
				}
			}
		} else {
			reply.VoteGranted = false
		}
	}
	//Send Reply on the channel
	reply.Term = r.getCurrentTerm()
	msg.reply <- reply
	r.DBG(" Handler Request Vote Exit\n")
}

//
//This function is called when RequestVote is received when a node is
// in candidate or leader state
//
func (r *RaftNode) handleCandidateOrLeaderRequestVote(msg RequestVoteMsg) bool {
	// TODO: Students should implement this method
	r.INF(" Handler Competing Request Vote Enter \n")
	retVal := false
	reply := RequestVoteReply{}
	if r.getCurrentTerm() > msg.args.Term {
		reply.VoteGranted = false
		r.INF("Term Greated VoteGranted=False\n")
		if r.state != FOLLOWER {
			retVal = true
		}
	} else {
		if r.getCurrentTerm() < msg.args.Term {
			r.setCurrentTerm(msg.args.Term)
			r.setVotedFor("")
		}
		if r.getVotedFor() == msg.args.CandidateId || r.getVotedFor() == "" {
			if r.GetLastLogTerm() > msg.args.LastLogTerm {
				reply.VoteGranted = false
				retVal = true
			} else if r.GetLastLogTerm() < msg.args.LastLogTerm {
				reply.VoteGranted = true
				r.setVotedFor(msg.args.CandidateId)
			} else {
				if r.GetLastLogIndex() > msg.args.LastLogIdx {
					reply.VoteGranted = false
					retVal = true
				} else {
					reply.VoteGranted = true
					r.setVotedFor(msg.args.CandidateId)
				}
			}
		} else {
			reply.VoteGranted = false
			retVal = true
		}
	}
	//Send Reply on the channel
	reply.Term = r.getCurrentTerm()
	msg.reply <- reply
	r.DBG(" Handler Competing Request Vote Exit\n")
	return retVal
}

//
//This routine wakes up periodically to apply any committed entries in the log
//
func (r *RaftNode) UpdateSM() {
	for {
		select {
		case <-r.makeUpdateSMPeriod():
			for r.commitIndex > r.lastApplied {
				r.lastApplied++
			}
		}
	}
}

//
//This function is called when node is leader.
//It updates commit index on the leader node as per the description
//in raft paper
//
func (r *RaftNode) UpdateCommitIdx() {
	//Commit any Entries that have been replicated on majority of peers
	//Find 'n+1' highest including from leader
	var mIdx []int
	mIdx = make([]int, 0, len(r.matchIndex))
	for _, v := range r.matchIndex {
		mIdx = append(mIdx, v)
	}
	sort.Ints(mIdx)

	//Find the min among the '(n+1/2)'  highest entries
	r.DBG("mIdx=%v", mIdx)
	N := mIdx[r.config.ClusterSize-1]
	for n := r.config.ClusterSize - 1; n >= r.config.Majority-1; n-- {
		if N > mIdx[n] {
			N = mIdx[n]
		}
	}
	r.DBG("			 Updt CommitIdx N=%d, mIdx=%v\n", N, mIdx)
	if (r.Log[N].Term == r.CurrentTerm) && r.commitIndex < N {
		r.commitIndex = N
		r.DBG("		 Updt CommitIdx: %d\n", r.commitIndex)
	}
}
