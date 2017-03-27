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

	//Set up network config
	for _, n := range r.othersAddr {
		r.netConfig.SetNetworkConfig(r.localAddr, *n, true)
	}

	//Kick of another go routine to apply any commited entries in the log
	go r.UpdateSM()

	//Main raft_server loop
	for {
		if r.nextState == RaftState_FOLLOWER {
			r.setState(RaftState_FOLLOWER)
			r.nextState = r.do_follower()
			r.INF(" Follower State Exit\n")
		} else if r.nextState == RaftState_CANDIDATE {
			r.setState(RaftState_CANDIDATE)
			r.nextState = r.do_candidate()
			r.INF(" Candidate State Exit\n")
		} else if r.nextState == RaftState_LEADER {
			r.setState(RaftState_LEADER)
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

		case msg := <-r.clientRequestMsgCh:
			r.handleClientRequest(msg)
		case msg := <-r.clientRegisterMsgCh:
			r.handleClientRegister(msg)
		case msg := <-r.appendEntriesMsgCh:
			r.DBG(" Rcv on AppendEntriesMsgCh\n")
			fallback := r.handleAppendEntries(msg)
			if fallback != true {
				r.leaderNode = *msg.args.FromNode
				r.INF(" From Leader \n")
				r.UpdateSM()
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
				r.INF(" Follower Rcv on ElecTO\n")
				rcvAppendEntries = false
			} else {
				return RaftState_CANDIDATE
			}
		}
	}
}

//
//This functions handles RPCs and other actions when raft server
//is in RaftState_CANDIDATE state
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
		case msg := <-r.clientRequestMsgCh:
			r.handleClientRequest(msg)
		case msg := <-r.clientRegisterMsgCh:
			r.handleClientRegister(msg)
		case msg := <-r.appendEntriesMsgCh:
			r.DBG(" Rcv on AppendEntriesMsgCh\n")
			if r.handleAppendEntries(msg) == true {
				r.leaderNode = *msg.args.FromNode
				r.UpdateSM()
				return RaftState_FOLLOWER
			}
		case msg := <-r.requestVoteMsgCh:
			r.DBG(" Rcv on RequestVoteMsgCh\n")
			if r.handleCandidateOrLeaderRequestVote(msg) == true {
				return RaftState_FOLLOWER
			}
		case <-r.appendEntriesReplyCh:
			//Do nothing
		case msg := <-r.requestVoteReplyCh:
			if msg.Term > r.getCurrentTerm() {
				r.setCurrentTerm(msg.Term)
				return RaftState_FOLLOWER
			}
			if msg.VoteGranted == true {
				voteCnt++
				r.INF(" Vote Resp, cnt=%d\n", voteCnt, msg)
				if voteCnt >= r.config.Majority {
					return RaftState_LEADER
				}
			}
		case <-r.makeElectionTimeout():
			r.INF(" Rcv on ElecTO\n")
			return RaftState_CANDIDATE
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
		if *n != r.localAddr {
			r.nextIndex[*n] = r.GetLastLogIndex() + 1
			r.matchIndex[*n] = r.GetLastLogIndex()
		}
	}
	sendTimeout := r.makeHeartbeatTimeout()
	fallback := make(chan bool)
	sentToMajority := make(chan bool, 1)
	sentToMajority <- true
	r.leaderNode = r.localAddr
	for {
		select {
		case <-sendTimeout:
			select {
			case <-sentToMajority:
				r.sendHeartBeats(fallback, sentToMajority)
				r.UpdateCommitIdx()
				r.UpdateSM()
				sendTimeout = r.makeHeartbeatTimeout()
			default:
				sendTimeout = time.After(time.Millisecond * 1)
			}
		case <-fallback:
			return RaftState_FOLLOWER
		case msg := <-r.appendEntriesMsgCh:
			if r.handleAppendEntries(msg) == true {
				r.UpdateSM()
				return RaftState_FOLLOWER
			}
		case msg := <-r.requestVoteMsgCh:
			if r.handleCandidateOrLeaderRequestVote(msg) == true {
				return RaftState_FOLLOWER
			}
		case <-r.appendEntriesReplyCh:
			r.INF(" Leader Heard on appendEntriesReplyCh")
		case <-r.requestVoteReplyCh:
			r.INF(" Leader Heard on requestVoteReplyCh")
		case msg := <-r.clientRequestMsgCh:
			r.handleClientRequest(msg)
		case msg := <-r.clientRegisterMsgCh:
			r.handleClientRegister(msg)
		}

	}
}

//
//In separate go routine for each peer, it sends RequestVote RPC to a peer,
//waits for the response and then forwards it to the server main-loop
//
func (r *RaftNode) requestVotes() {
	r.INF(" Starting Election\n")
	args := RequestVoteArgs{}
	args.FromNode = &r.localAddr
	args.Term = r.getCurrentTerm()
	args.CandidateId = r.Id
	args.LastLogIdx = r.GetLastLogIndex()
	args.LastLogTerm = r.GetLogEntry(args.LastLogIdx).Term
	for _, n := range r.othersAddr {
		if *n != r.localAddr {
			go func(n NodeAddr) {
				//reply := &RequestVoteReply{}
				r.INF(" Send ReqVote to %v\n", n.Id)
				//reply, err := r.RequestVoteRPC(&n, args)
				reply, err := RequestVoteRPC(&n, args)
				if err == nil {
					r.requestVoteReplyCh <- *reply
				}
			}(*n)
		}
	}
	return
}

//
//This function is called by the leader to sendHeartbeats.
//If a follower is up to date, then it simply send appendEntries
//with no entries(empty). Otherwise, it sends the follower the
//newer entries
func (r *RaftNode) sendHeartBeats(fallBack chan bool, sentToMajority chan bool) {
	go func() {
		r.INF(" Send Heartbeats Enter\n")

		term := r.getCurrentTerm()
		successCnt := 1
		r.matchIndex[r.localAddr] = r.GetLastLogIndex()
		nodes := r.othersAddr

		for _, pn := range nodes {
			n := *pn
			if r.getState() != RaftState_LEADER {
				return
			}
			if n != r.localAddr {
				req := AppendEntriesArgs{}
				req.FromNode = &r.localAddr
				req.Term = term
				req.LeaderId = r.Id
				req.PrevLogIdx = r.nextIndex[n] - 1
				req.PrevLogTerm = r.GetLogEntry(r.nextIndex[n] - 1).Term
				if r.GetLastLogIndex() >= r.nextIndex[n] {
					r.DBG(" AppendEntries[%d] PrevLogIdx:%d\n", n, req.PrevLogIdx)
					req.Entries = make([]*LogEntry, r.GetLastLogIndex()-r.nextIndex[n]+1)
					for i, j := r.nextIndex[n], 0; i <= r.GetLastLogIndex(); i, j = i+1, j+1 {
						req.Entries[j] = r.GetLogEntry(i)
					}
				} else {
					req.Entries = make([]*LogEntry, 0)
				}
				req.LeaderCommit = r.commitIndex

				r.INF(" Send AE to %v\n", n.Id)
				//reply, err := r.AppendEntriesRPC(&n, req)
				reply, err := AppendEntriesRPC(&n, req)
				if err == nil {
					if reply.Term > r.getCurrentTerm() {
						r.setCurrentTerm(reply.Term)
						fallBack <- true
						sentToMajority <- false
					}
					if reply.Success == true {
						successCnt++
						r.nextIndex[n] = req.PrevLogIdx + int32(len(req.Entries)) + 1
						r.matchIndex[n] = req.PrevLogIdx + int32(len(req.Entries))
					} else {
						if r.nextIndex[n] > 1 {
							r.nextIndex[n]--
						}
					}
				} else {
					r.INF("Error sending AET to %v", n.Id)
				}
			}
		}
		sentToMajority <- true
		r.INF(" Send Heartbeats Exit \n")
	}()
}

// This function creates timer channel with random timeout.
func (r *RaftNode) makeElectionTimeout() <-chan time.Time {
	val := rand.Int()
	//	val = val + 100*rand.Int()
	//	if val < 0 {
	//		val *= -1
	//	}
	to := (val % r.config.ElectionTimeout) + r.config.ElectionTimeoutBase
	//	r.INF("ElectionTimeout=%d\n", to)
	return time.After(time.Duration(to) * time.Millisecond)
}

//This functions creates timer channel with heartBeat timeout
func (r *RaftNode) makeHeartbeatTimeout() <-chan time.Time {
	return time.After(time.Duration(r.config.HeartbeatFrequency) * time.Millisecond)
}

//Period for UpdateSM go routine to be woken up
func (r *RaftNode) makeUpdateSMPeriod() <-chan time.Time {
	return time.After(time.Duration(100) * time.Millisecond)
}

//
//This function handles AppendEntries RPC as per the description in raft paper
//
func (r *RaftNode) handleAppendEntries(msg AppendEntriesMsg) bool {
	retVal := false
	reply := AppendEntriesReply{}
	r.INF(" handleAppendEntries Enter\n")
	r.DBG(" 	Term=%d, NumEnt=%d, LeadId=%s, PrevLogIdx=%d,PrevLogTerm=%d,LeadCom=%d\n", msg.args.Term, len(msg.args.Entries),
		msg.args.LeaderId, msg.args.PrevLogIdx, msg.args.PrevLogTerm, msg.args.LeaderCommit)

	//Update our term if greater than the current term
	if msg.args.Term > r.getCurrentTerm() {
		r.setCurrentTerm(msg.args.Term)
		if r.state != RaftState_FOLLOWER {
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
	r.INF(" handleAppendEntries Exit, reply=%v\n", reply)
	return retVal
}

//
//handleRequestVote
//
func (r *RaftNode) handleRequestVote(msg RequestVoteMsg) {
	r.INF("Request Vote Enter from %v\n", msg.args.CandidateId)
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
	r.INF("Request Vote Exit\n")
}

//
//This function is called when RequestVote is received when a node is
// in candidate or leader state
//
func (r *RaftNode) handleCandidateOrLeaderRequestVote(msg RequestVoteMsg) bool {
	r.INF("Competing Request Vote Enter %v\n", msg.args.CandidateId)
	retVal := false
	reply := RequestVoteReply{}
	if r.getCurrentTerm() > msg.args.Term {
		reply.VoteGranted = false
		r.INF("Term Greated VoteGranted=False\n")
		if r.state != RaftState_FOLLOWER {
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
	r.INF(" Handler Competing Request Vote Exit\n")
	return retVal
}

func (r *RaftNode) handleClientRegister(msg ClientRegisterMsg) {
	if r.getState() == RaftState_FOLLOWER {
		reply := ClientRegisterReply{Code: ClientReplyCode_NOT_LEADER, ClientId: -1, LeaderNode: &r.leaderNode}
		msg.reply <- reply
	} else if r.getState() == RaftState_CANDIDATE {
		reply := ClientRegisterReply{Code: ClientReplyCode_RETRY, ClientId: -1, LeaderNode: &r.leaderNode}
		msg.reply <- reply
	} else if r.getState() == RaftState_LEADER {
		//appendLogEntry
		op := &Operation{Type: OpType_CLIENT_REGISTRATION, Key: "", Value: ""}
		cmd := &Command{ClientId: -1, SeqNum: 0, Op: op}
		entry := &LogEntry{Term: r.getCurrentTerm(), Cmd: cmd}
		r.AppendLog(entry)
		r.clientRegisterMap[r.GetLastLogIndex()] = msg
		r.INF("ClientRegistration AppendLog")
	}
}

func (r *RaftNode) handleClientRequest(msg ClientRequestMsg) {
	if r.getState() == RaftState_FOLLOWER {
		reply := ClientReply{Code: ClientReplyCode_NOT_LEADER, LeaderNode: &r.leaderNode, Value: ""}
		msg.reply <- reply
	} else if r.getState() == RaftState_CANDIDATE {
		reply := ClientReply{Code: ClientReplyCode_RETRY, LeaderNode: &r.leaderNode, Value: ""}
		msg.reply <- reply
	} else if r.getState() == RaftState_LEADER {
		//If client is registered
		e := r.GetLogEntry(msg.args.Cmd.ClientId)
		if e.Cmd.Op.Type == OpType_CLIENT_REGISTRATION {
			//Check the last response sent to client
			replyEntry, ok := r.clientAppliedMap[msg.args.Cmd.ClientId]
			if !ok {
				//appendLogEntry
				entry := &LogEntry{Term: r.getCurrentTerm(), Cmd: msg.args.Cmd}
				r.AppendLog(entry)
				r.clientRequestMap[r.GetLastLogIndex()] = msg
				r.INF("ClientRequest Appended")
			} else {
				if msg.args.Cmd.SeqNum == replyEntry.SeqNum {
					//Reply REQUEST_SUCCESSFUL
					reply := ClientReply{Code: ClientReplyCode_REQUEST_SUCCESSFUL, LeaderNode: &r.leaderNode, Value: replyEntry.Value}
					msg.reply <- reply
					r.INF("Client Request Replayed")
				} else if msg.args.Cmd.SeqNum > replyEntry.SeqNum {
					//appendLogEntry
					entry := &LogEntry{Term: r.getCurrentTerm(), Cmd: msg.args.Cmd}
					r.AppendLog(entry)
					r.clientRequestMap[r.GetLastLogIndex()] = msg
					r.INF("ClientRequest Appended")
				} else {
					reply := ClientReply{Code: ClientReplyCode_REQUEST_FAILED, LeaderNode: &r.leaderNode, Value: ""}
					msg.reply <- reply
					r.INF("ClientRequest Bad")
				}
			}
		} else {
			//REQUEST_FAILED
			reply := ClientReply{Code: ClientReplyCode_REQUEST_FAILED, LeaderNode: &r.leaderNode, Value: ""}
			msg.reply <- reply
			r.INF("ClientRequest NOT Registered")
		}
	}
}

//
//This routine wakes up periodically to apply any committed entries in the log
//
func (r *RaftNode) UpdateSM() {

	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry := r.GetLogEntry(r.lastApplied)
		cmd := *entry.Cmd
		if cmd.Op.Type == OpType_CLIENT_REGISTRATION {
			r.INF("SM: ClientRegistration Applied")
			reply := ClientRegisterReply{}
			reply.ClientId = r.lastApplied
			reply.LeaderNode = &r.localAddr
			reply.Code = ClientReplyCode_REQUEST_SUCCESSFUL
			if msg, ok := r.clientRegisterMap[r.lastApplied]; ok {
				r.INF("Replying to ClientRegister")
				msg.reply <- reply
				delete(r.clientRegisterMap, r.lastApplied)
			}
		} else {
			value, err := r.app.ApplyOperation(*entry.Cmd)
			reply := ClientReply{}
			if err != nil {
				reply.Code = ClientReplyCode_REQUEST_FAILED
			} else {
				r.INF("SM: ClientRequest Applied")
				reply.Code = ClientReplyCode_REQUEST_SUCCESSFUL
			}
			reply.LeaderNode = &r.leaderNode
			reply.SeqNum = entry.Cmd.SeqNum
			reply.Value = value
			r.clientAppliedMap[entry.Cmd.ClientId] = reply
			if msg, ok := r.clientRequestMap[r.lastApplied]; ok {
				r.INF("Replying to client")
				msg.reply <- reply
				delete(r.clientRequestMap, r.lastApplied)
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
		mIdx = append(mIdx, int(v))
	}
	sort.Ints(mIdx)

	//Find the min among the '(n+1/2)'  highest entries
	//r.DBG("mIdx=%v", mIdx)
	N := mIdx[r.config.ClusterSize-1]
	for n := r.config.ClusterSize - 1; n >= r.config.Majority-1; n-- {
		if N > mIdx[n] {
			N = mIdx[n]
		}
	}
	//r.DBG("			 Updt CommitIdx N=%d, mIdx=%v\n", N, mIdx)
	if (r.Log[N].Term == r.CurrentTerm) && r.commitIndex < int32(N) {
		r.commitIndex = int32(N)
		r.DBG("		 Updt CommitIdx: %d\n", r.commitIndex)
	}
}
