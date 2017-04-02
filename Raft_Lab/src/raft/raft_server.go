package raft

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

type ElectionResultMsg struct {
	result bool
	term   int
}

//
//Main raft_server go routine
//
func (rf *Raft) run_server() {
	DPrintf("Serv[%d], Server Starting\n", rf.me)

	//Kick of another go routine to apply any commited entries in the log
	go rf.UpdateSM()

	//Main raft_server loop
	for {
		if rf.nextState == FOLLOWER {
			rf.setState(FOLLOWER)
			rf.nextState = rf.do_follower()
			DPrintf("Serv[%d], Follower State Exit\n", rf.me)
		} else if rf.nextState == CANDIDATE {
			rf.setState(CANDIDATE)
			rf.nextState = rf.do_candidate()
			DPrintf("Serv[%d], Candidate State Exit\n", rf.me)
		} else if rf.nextState == LEADER {
			rf.setState(LEADER)
			rf.nextState = rf.do_leader()
			DPrintf("Serv[%d], Leader State Exit\n", rf.me)
		} else {
			//fatal error
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in FOLLOWER state
//
func (rf *Raft) do_follower() (nextState RaftState) {
	DPrintf("Serv[%d], Follower State Enter\n", rf.me)
	rcvAppendEntries := false
	for {
		select {
		case msg := <-rf.appendEntriesMsgCh:
			DPrintf("Serv[%d], Rcv on AppendEntriesMsgCh\n", rf.me)
			fallback := rf.handleAppendEntries(msg)
			if fallback != true {
				DPrintf("Serv[%d], From Leader\n", rf.me)
				rcvAppendEntries = true
			}
		case msg := <-rf.requestVoteMsgCh:
			DPrintf("Serv[%d], Rcv on RequestVoteMsgCh\n", rf.me)
			rf.handleRequestVote(msg)
		case <-rf.appendEntriesReplyCh:
			//Do nothing
		case <-rf.requestVoteReplyCh:
			//Do nothing
		case <-rf.makeElectionTimeout():
			if rcvAppendEntries == true {
				DPrintf("Serv[%d], Rcv on ElecTO\n", rf.me)
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
func (rf *Raft) do_candidate() (nextState RaftState) {
	DPrintf("Serv[%d], Candidate State \n", rf.me)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setVotedFor(rf.me)
	voteCnt := 1
	rf.requestVotes()
	DPrintf("Serv[%d], Wait in Candidate State\n", rf.me)
	for {
		select {
		case msg := <-rf.appendEntriesMsgCh:
			DPrintf("Serv[%d], Rcv on AppendEntriesMsgCh\n", rf.me)
			rf.handleAppendEntries(msg)
		case msg := <-rf.requestVoteMsgCh:
			DPrintf("Serv[%d], Rcv on RequestVoteMsgCh\n", rf.me)
			if rf.handleCandidateOrLeaderRequestVote(msg) != true {
				return FOLLOWER
			}
		case <-rf.appendEntriesReplyCh:
			//Do nothing
		case msg := <-rf.requestVoteReplyCh:
			if msg.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(msg.Term)
				return FOLLOWER
			}
			if msg.VoteGranted == true {
				voteCnt++
				DPrintf("Serv[%d], Vote Resp, cnt=%d\n", rf.me, voteCnt)
				if voteCnt >= GetMajority(len(rf.peers)) {
					return LEADER
				}
			}
		case <-rf.makeElectionTimeout():
			DPrintf("Serv[%d], Rcv on ElecTO\n", rf.me)
			return CANDIDATE
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in LEADER state
//
func (rf *Raft) do_leader() (nextState RaftState) {
	DPrintf("Serv[%d], Leader State Enter\n", rf.me)
	for p := 0; p < len(rf.peers); p++ {
		rf.nextIndex[p] = rf.GetLastLogIndex() + 1
		if p != rf.me {
			rf.matchIndex[p] = 0
		} else {
			rf.matchIndex[p] = rf.GetLastLogIndex()
		}
	}
	notMajorityCnt := 0
	for {
		select {
		case msg := <-rf.appendEntriesMsgCh:
			if rf.handleAppendEntries(msg) == true {
				return FOLLOWER
			}
		case msg := <-rf.requestVoteMsgCh:
			if rf.handleCandidateOrLeaderRequestVote(msg) != true {
				return FOLLOWER
			}
		case <-rf.appendEntriesReplyCh:
			//\TODO
		case <-rf.requestVoteReplyCh:
			//Do nothing
		case <-rf.makeHeartbeatTimeout():
			fallBack, sentToMajority := rf.sendHeartBeats()
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
		rf.UpdateCommitIdx()
	}
}

//
//In separate go routine for each peer, it sends RequestVote RPC to a peer,
//waits for the response and then forwards it to the server main-loop
//
func (rf *Raft) requestVotes() {
	DPrintf("Serv[%d], Starting Election\n", rf.me)
	args := RequestVoteArgs{}
	args.Term = rf.getCurrentTerm()
	args.CandidateId = rf.me
	args.LastLogIdx = rf.GetLastLogIndex()
	args.LastLogTerm = rf.GetLogEntry(args.LastLogIdx).Term
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			go func(p int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(p, args, reply)
				if ok == true {
					rf.requestVoteReplyCh <- *reply
				}
			}(p)
		}
	}
	return
}

//
//This function is called by the leader to sendHeartbeats.
//If a follower is up to date, then it simply send appendEntries
//with no entries(empty). Otherwise, it sends the follower the
//newer entries
func (rf *Raft) sendHeartBeats() (fallBack, sentToMajority bool) {
	DPrintf("Serv[%d], Send Heartbeats Enter\n", rf.me)

	term := rf.getCurrentTerm()
	successCnt := 1
	rf.matchIndex[rf.me] = rf.GetLastLogIndex()

	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			req := AppendEntriesArgs{}
			req.Term = term
			req.LeaderId = rf.me
			reply := &AppendEntriesReply{}
			req.PrevLogIdx = rf.nextIndex[p] - 1
			req.PrevLogTerm = rf.GetLogEntry(rf.nextIndex[p] - 1).Term
			if rf.GetLastLogIndex() >= rf.nextIndex[p] {
				DPrintf("Serv[%d], AppendEntries[%d] PrevLogIdx:%d\n", rf.me, p, req.PrevLogIdx)

				req.Entries = make([]LogEntry, rf.GetLastLogIndex()-rf.nextIndex[p]+1)
				for i, j := rf.nextIndex[p], 0; i <= rf.GetLastLogIndex(); i, j = i+1, j+1 {
					req.Entries[j] = rf.GetLogEntry(i)
				}
			} else {
				req.Entries = nil
			}
			req.LeaderCommit = (int)(atomic.LoadInt64(&rf.commitIndex))

			ok := rf.sendAppendEntries(p, req, reply)
			if ok == true {
				if reply.Term > rf.getCurrentTerm() {
					rf.setCurrentTerm(reply.Term)
					fallBack = true
					sentToMajority = false
					DPrintf("Serv[%d], Send Heartbeats Exit %v %v\n", rf.me, fallBack, sentToMajority)
					return fallBack, sentToMajority
				}
				successCnt++
				if reply.Success == true {
					rf.nextIndex[p] = req.PrevLogIdx + len(req.Entries) + 1
					rf.matchIndex[p] = req.PrevLogIdx + len(req.Entries)
				} else {
					if rf.nextIndex[p] > 1 {
						rf.nextIndex[p]--
					}
				}
			}
		}
	}
	if successCnt >= GetMajority(len(rf.peers)) {
		fallBack = false
		sentToMajority = true
	} else {
		fallBack = false
		sentToMajority = false
	}

	DPrintf("Serv[%d], Send Heartbeats Exit %v %v\n", rf.me, fallBack, sentToMajority)
	return fallBack, sentToMajority
}

// This function creates timer channel with random timeout.
func (rf *Raft) makeElectionTimeout() <-chan time.Time {
	return time.After(time.Duration(((rand.Int() % rf.electionTimeout) + rf.electionTimeout)) * time.Millisecond)
}

//This functions creates timer channel with heartBeat timeout
func (rf *Raft) makeHeartbeatTimeout() <-chan time.Time {
	return time.After(time.Duration(rf.heartbeatFrequency) * time.Millisecond)
}

//Period for UpdateSM go routine to be woken up
func (rf *Raft) makeUpdateSMPeriod() <-chan time.Time {
	return time.After(time.Duration(10) * time.Millisecond)
}

//
//This function handles AppendEntries RPC as per the description in raft paper
//
func (rf *Raft) handleAppendEntries(msg AppendEntriesMsg) bool {
	retVal := false
	reply := AppendEntriesReply{}
	DPrintf("Serv[%d], handleAppendEntries Enter\n", rf.me)
	DPrintf("Serv[%d], 	Term=%d, NumEnt=%d, LeadId=%d, PrevLogIdx=%d,PrevLogTerm=%d,LeadCom=%d\n", rf.me, msg.args.Term, len(msg.args.Entries),
		msg.args.LeaderId, msg.args.PrevLogIdx, msg.args.PrevLogTerm, msg.args.LeaderCommit)

	//Update our term if greater than the current term
	if msg.args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(msg.args.Term)
		if rf.state != FOLLOWER {
			retVal = true
		}
	}

	//If AppendEntriesRequest term is smaller
	if msg.args.Term < rf.getCurrentTerm() {
		//Send Reply on the channel
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		msg.reply <- reply
		DPrintf("Serv[%d], handleAppendEntries Exit, reply=%v\n", rf.me, reply)
		return retVal
	}

	//If PrevLogTerm doesn't match with term of entry at PrevLogIndex
	if msg.args.PrevLogIdx > rf.GetLastLogIndex() ||
		msg.args.PrevLogTerm != rf.GetLogEntry(msg.args.PrevLogIdx).Term {
		//Send Reply on the channel
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
		msg.reply <- reply
		DPrintf("Serv[%d], handleAppendEntries Exit, reply=%v\n", rf.me, reply)
		return retVal
	}

	//Replace conflicting entries and append new entries
	i := msg.args.PrevLogIdx
	ei := 0
	for ei = 0; i < rf.GetLastLogIndex() && ei < len(msg.args.Entries); ei++ {
		i++
		if rf.GetLogEntry(i).Term != msg.args.Entries[ei].Term {
			break
		}
	}
	rf.RemoveLogEntry(i)
	for ; ei < len(msg.args.Entries); ei++ {
		i++
		DPrintf("Serv[%d], Appended@%d\n", rf.me, i)
		rf.AppendLog(msg.args.Entries[ei])
		//rf.log[i] = msg.args.Entries[ei]
	}

	//Update commit index
	if msg.args.LeaderCommit > int(rf.commitIndex) {
		if msg.args.LeaderCommit > (i - 1) {
			rf.commitIndex = int64(msg.args.LeaderCommit)
		} else if msg.args.LeaderCommit < (i - 1) {
			rf.commitIndex = int64(i - 1)
		}
		DPrintf("Serv[%d], Updt CommitIdx: %d\n", rf.me, rf.commitIndex)
	}

	//Send Reply on the channel
	reply.Term = rf.getCurrentTerm()
	reply.Success = true
	msg.reply <- reply
	DPrintf("Serv[%d], handleAppendEntries Exit, reply=%v\n", rf.me, reply)
	return retVal
}

//
//handleRequestVote
//
func (rf *Raft) handleRequestVote(msg RequestVoteMsg) {
	DPrintf("Serv[%d], Handler Request Vote Enter %v\n", rf.me, msg.args)
	reply := RequestVoteReply{}

	if rf.getCurrentTerm() > msg.args.Term {
		reply.VoteGranted = false
	} else {
		if rf.getCurrentTerm() < msg.args.Term {
			rf.setCurrentTerm(msg.args.Term)
			rf.setVotedFor(-1)
		}
		if rf.getVotedFor() == msg.args.CandidateId || rf.getVotedFor() == -1 {
			if rf.GetLastLogTerm() > msg.args.LastLogTerm {
				reply.VoteGranted = false
			} else if rf.GetLastLogTerm() < msg.args.LastLogTerm {
				reply.VoteGranted = true
				rf.setVotedFor(msg.args.CandidateId)
			} else {
				if rf.GetLastLogIndex() > msg.args.LastLogIdx {
					reply.VoteGranted = false
				} else {
					reply.VoteGranted = true
					rf.setVotedFor(msg.args.CandidateId)
				}
			}
		} else {
			reply.VoteGranted = false
		}
	}
	//Send Reply on the channel
	reply.Term = rf.getCurrentTerm()
	msg.reply <- reply
	DPrintf("Serv[%d], Handler Request Vote Exit\n", rf.me)
}

//
//This function is called when RequestVote is received when a node is
// in candidate or leader state
//
func (rf *Raft) handleCandidateOrLeaderRequestVote(msg RequestVoteMsg) bool {
	// TODO: Students should implement this method
	DPrintf("Serv[%d], Handler Competing Request Vote Enter \n", rf.me)
	retVal := false
	reply := RequestVoteReply{}
	if rf.getCurrentTerm() > msg.args.Term {
		reply.VoteGranted = false
		if rf.state != FOLLOWER {
			retVal = true
		}
	} else {
		if rf.getCurrentTerm() < msg.args.Term {
			rf.setCurrentTerm(msg.args.Term)
			rf.setVotedFor(-1)
		}
		if rf.getVotedFor() == msg.args.CandidateId || rf.getVotedFor() == -1 {
			if rf.GetLastLogTerm() > msg.args.LastLogTerm {
				reply.VoteGranted = false
				retVal = true
			} else if rf.GetLastLogTerm() < msg.args.LastLogTerm {
				reply.VoteGranted = true
				rf.setVotedFor(msg.args.CandidateId)
			} else {
				if rf.GetLastLogIndex() > msg.args.LastLogIdx {
					reply.VoteGranted = false
					retVal = true
				} else {
					reply.VoteGranted = true
					rf.setVotedFor(msg.args.CandidateId)
				}
			}
		} else {
			reply.VoteGranted = false
			retVal = true
		}
	}
	//Send Reply on the channel
	reply.Term = rf.getCurrentTerm()
	msg.reply <- reply
	DPrintf("Serv[%d], Handler Competing Request Vote Exit\n", rf.me)
	return retVal
}

//
//This routine wakes up periodically to apply any committed entries in the log
//
func (rf *Raft) UpdateSM() {
	for {
		select {
		case <-rf.makeUpdateSMPeriod():
			cmtIdx := (int)(atomic.LoadInt64(&rf.commitIndex))
			DPrintf("Serv[%d]: UpdateSM CmtIdx=%d, LastApp: %d\n", rf.me, cmtIdx, rf.lastApplied)

			for cmtIdx > rf.lastApplied {
				rf.lastApplied++
				DPrintf("Serv[%d]: UpdateSM, Log: %v\n", rf.me, rf.Log)
				DPrintf("Serv[%d]: UpdateSM  LastApplied: %d\n", rf.me, rf.lastApplied)
				rf.applyMsgCh <- ApplyMsg{Index: rf.lastApplied, Command: rf.GetLogEntry(rf.lastApplied).Cmd}
			}
		}
	}
}

//
//This function is called when node is leader.
//It updates commit index on the leader node as per the description
//in raft paper
//
func (rf *Raft) UpdateCommitIdx() {
	//Commit any Entries that have been replicated on majority of peers
	//Find 'n+1' highest including from leader
	var mIdx []int
	mIdx = make([]int, 0, len(rf.matchIndex))
	for _, v := range rf.matchIndex {
		mIdx = append(mIdx, v)
	}
	sort.Ints(mIdx)

	//Find the min among the '(n+1/2)'  highest entries
	N := mIdx[len(rf.peers)-1]
	for n := len(rf.peers) - 1; n >= GetMajority(len(rf.peers))-1; n-- {
		if N > mIdx[n] {
			N = mIdx[n]
		}
	}
	DPrintf("Update CommitIdx: Serv[%d], Updt CommitIdx N=%d, mIdx=%v\n", rf.me, N, mIdx)
	if (rf.Log[N].Term == rf.CurrentTerm) && int(rf.commitIndex) < N {
		atomic.StoreInt64(&rf.commitIndex, int64(N))
		DPrintf("Update CommitIdx: Serv[%d], Updt CommitIdx: %d\n", rf.me, rf.commitIndex)
	}
}
