// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"reflect"

	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// leader is changing leadership and reject propose new log entry temporarily
var ErrLeadershipChange = errors.New("leadership changing")
// there has been a pending conf change log entry
var ErrPendingConfChange = errors.New("leadership changing")



// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
	Log func(string, ...interface{}) string
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next   uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// grantVotes records
	grantVotes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// ticks
	ticks   int
	readyUpdated bool
	*SoftState
	pb.HardState

	log func(string, ...interface{}) string
}

func PanicErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func resetElapsed(elapsed *int, ticks *int, baseline int, randomized bool) {
	*elapsed = baseline
	if randomized {
		*elapsed += rand.Intn(baseline)
	}
	*ticks = 0
}

func (r *Raft) newProgress(id uint64) *Progress {
	if r.id == id {
		return &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
	} else {
		return &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	}
}

func (r *Raft) initPrs(peers []uint64, useOld bool) {
	newPrs := make(map[uint64]*Progress)
	for _, id := range peers {
		if Pr, exist := r.Prs[id]; useOld && exist {
			newPrs[id] = Pr
		} else {
			newPrs[id] = r.newProgress(id)
		}
	}
//	newPrs[r.id] = r.newProgress(r.id)
	r.Prs = newPrs
}

func (r *Raft) getPeersFromPrs() []uint64 {
	ret := make([]uint64, 0)
	for id := range r.Prs {
		ret = append(ret, id)
	}
	return ret
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, conf, err := c.Storage.InitialState()
	PanicErr(err)
	logFunc := func(s string, i ...interface{}) string {
		if c.Log == nil {
			log.Infof(s, i...)
		} else {
			c.Log(s, i...)
		}
		return ""
	}
	raftLog := newLogWithLogFunc(c.Storage, logFunc)
	raftLog.applied = max(raftLog.applied, c.Applied)
	var peers []uint64
	if c.peers == nil {
		peers = conf.GetNodes()
	} else {
		// For project2a
		peers = c.peers
	}
	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          raftLog,
		State:            StateFollower,
		Vote:             hardState.Vote,
		Term:             hardState.Term,
		SoftState: &SoftState{Lead: None, RaftState: StateFollower},
		HardState: hardState,
		msgs: make([]pb.Message, 0),
		log: logFunc,
	}
	raft.log("%d init Peers=%+v", c.ID, peers)
	raft.initPrs(peers, false)
	resetElapsed(&raft.electionElapsed, &raft.ticks, raft.electionTimeout, true)

	return raft
}

func (r *Raft) sendMessage(to uint64, MsgType pb.MessageType, m pb.Message) {
	r.readyUpdated = true
	m.From = r.id
	m.Term = r.Term
	m.To = to
	m.MsgType = MsgType
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	p := r.Prs[to]
	ents := r.RaftLog.GetEntries(p.Next, r.RaftLog.LastIndex()+1, false)
	prevIndex := p.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		// prev entry isn't exist, maybe compact,
		// so send snapshot
		//if r.RaftLog.pendingSnapshot == nil {
			// maybe node restart, so get a new snapshot
			snapshot, err := r.RaftLog.storage.Snapshot()
			r.log("%d -> %d, index=%d not found, snapIndex,Term=%d:%d, pending=%+v",
		 		r.id, to, prevIndex, r.RaftLog.snapIndex, r.RaftLog.snapTerm, snapshot)
			if err != nil {
				return false
			}
		//	r.RaftLog.pendingSnapshot = &snap
		//}
		
		r.sendMessage(to, pb.MessageType_MsgSnapshot, pb.Message{
			Snapshot: &snapshot,
		})
		// for TestProvideSnap2C
		return true
	}

	r.sendMessage(to, pb.MessageType_MsgAppend, pb.Message{
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.sendMessage(to, pb.MessageType_MsgHeartbeat, pb.Message{})
}

func (r *Raft) sendVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	PanicErr(err)
	r.sendMessage(to, pb.MessageType_MsgRequestVote,
		pb.Message{
			Index:   lastIndex,
			LogTerm: lastTerm,
		})
}

func (r *Raft) sendVotesToOthers() {
	for id := range r.Prs {
		if id != r.id {
			r.sendVote(id)
		}
	}
}

func (r *Raft) sendHeartBeatToOthers() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) sendAppendToOthers() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.ticks++
	switch r.State {
	case StateLeader:
		if r.ticks == r.heartbeatElapsed {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			resetElapsed(&r.heartbeatElapsed, &r.ticks, r.heartbeatTimeout, false)
		}
	case StateFollower:
		if r.ticks == r.electionElapsed {
			r.log("%d new election newTerm=%v", r.id, r.Term + 1)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		if r.ticks == r.electionElapsed {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.readyUpdated = true
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	resetElapsed(&r.electionElapsed, &r.ticks, r.electionTimeout, true)
}

func (r *Raft) checkVote() {
	if len(r.grantVotes) > len(r.Prs) / 2 {
		voteCnt := 0
		for _, vote := range r.grantVotes {
			if vote {
				voteCnt++
			}
		}
		reject := len(r.grantVotes) - voteCnt
		if voteCnt > len(r.Prs) / 2 {
			r.log("%d voteCnt=%d, grantVotes=%+v, Prs=%+v, term=%v, -> Leader", r.id, voteCnt, r.grantVotes, r.Prs, r.Term)
			r.becomeLeader()
		} else if reject > len(r.Prs) / 2 {
			r.becomeFollower(r.Term, None)
		}
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.readyUpdated = true
	r.Term++
	r.State = StateCandidate
	r.grantVotes = make(map[uint64]bool)
	r.grantVotes[r.id] = true
	r.Vote = r.id
	resetElapsed(&r.electionElapsed, &r.ticks, r.electionTimeout, true)
	r.checkVote()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// If node was removed from group, it can't become leader
	if _, exist := r.Prs[r.id]; !exist {
		return
	}
	r.readyUpdated = true
	r.State = StateLeader
	resetElapsed(&r.heartbeatElapsed, &r.ticks, r.heartbeatTimeout, false)

	// reinit Progress
	r.initPrs(r.getPeersFromPrs(), false)

	// noop entry
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	r.leadTransferee = 0
	r.PendingConfIndex = 0
	r.Lead = r.id
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	higherLogTerm := func(m pb.Message, equal bool) bool {
		if m.Term > r.Term || (equal && m.Term == r.Term) {
			r.log("%d become follower because msg=%+v", r.id, m)
			r.becomeFollower(m.Term, None)
			return true
		}
		return false
	}

	switch r.State {
	case StateFollower:
		if higherLogTerm(m, false) {
			return r.Step(m)
		}
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVotesToOthers()
		case pb.MessageType_MsgAppend:
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.Lead = m.From
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			if _, exist := r.Prs[r.id]; exist {
				r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			}
		case pb.MessageType_MsgTransferLeader:
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	case StateCandidate:
		if higherLogTerm(m,
			m.MsgType == pb.MessageType_MsgHeartbeat ||
				m.MsgType == pb.MessageType_MsgAppend) {
			return r.Step(m)
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVotesToOthers()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			// reject
			r.handleRequestVote(m)
		}
	case StateLeader:
		if higherLogTerm(m, false) {
			return r.Step(m)
		}
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.sendHeartBeatToOthers()
		case pb.MessageType_MsgHeartbeatResponse:
			if m.Commit < r.RaftLog.committed {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgPropose:
			if r.leadTransferee != 0 {
				return ErrLeadershipChange
			}
			lastIndex := r.RaftLog.LastIndex()
			for i, ent := range m.Entries {
				ent.Index = uint64(i) + lastIndex + 1
				ent.Term = r.Term
				if ent.EntryType == pb.EntryType_EntryConfChange {
					if r.RaftLog.applied < r.PendingConfIndex {
						r.sendAppendToOthers()
						r.checkUpdateCommit()
						r.log("ErrPendingConfChange, applied=%v, pendingConfIndex=%v", r.RaftLog.applied, r.PendingConfIndex)
						return ErrPendingConfChange
					}
					r.log("new pendingConfIndex=%v", r.PendingConfIndex)
					r.PendingConfIndex = ent.Index
				}
				r.log("new log term=%v, index=%v", ent.Term, ent.Index)
			}
			r.RaftLog.Append(lastIndex, m.Entries...)
			// TestProgressLeader2AB need it, always make r.Prs[r.id].Match newest
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
			r.sendAppendToOthers()
			r.checkUpdateCommit()
		case pb.MessageType_MsgRequestVote:
			// reject
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgTransferLeader:
			if _, exist := r.Prs[m.From]; exist {
				r.leadTransferee = m.From
				r.checkTransferee()
			}
			
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// reject
	if m.Term < r.Term || !r.RaftLog.EntryMatch(m.LogTerm, m.Index) {
		r.sendMessage(m.From, pb.MessageType_MsgAppendResponse, pb.Message{
			Reject: true,
		})
		return
	}

	r.RaftLog.Append(m.Index, m.Entries...)
	var lastNewCommit uint64
	if len(m.Entries) > 0 {
		lastNewCommit = m.Entries[len(m.Entries) - 1].Index
	} else {
		lastNewCommit = m.Index
	}
	r.RaftLog.SetCommited(min(m.Commit, lastNewCommit))
	r.sendMessage(m.From, pb.MessageType_MsgAppendResponse, pb.Message{
		Reject: false,
		Index: r.RaftLog.LastIndex(),
	})
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	p := r.Prs[m.From]
	// r.log("%d appendResp: to=%d Prs=%+v, lastIndex=%d snapIndex=%d snapTerm=%d",
	//  r.id, m.From, p, r.RaftLog.LastIndex(), r.RaftLog.snapIndex, r.RaftLog.snapTerm)
	if m.Reject {
		p.Next--
		// send Append at now
		r.sendAppend(m.From)
	} else {
		p.Match = m.Index
		p.Next = m.Index + 1

		if m.From == r.leadTransferee {
			r.checkTransferee()
			return
		}

		if r.checkUpdateCommit() {
			// commit update, push to follower
			r.sendAppendToOthers()
			return
		}
		if p.Match < r.RaftLog.LastIndex() {
			// for test TestProvideSnap2C
			r.sendAppend(m.From)
		}
	}
}

// try update commit
func (r *Raft) checkUpdateCommit() bool {
	newCommit := r.RaftLog.committed + 1
	for ; newCommit <= r.RaftLog.LastIndex(); newCommit++ {
		cnt := 0
		for id := range r.Prs {
			if id != r.id && r.Prs[id].Match >= newCommit {
				cnt++
			}
		}
		if cnt + 1 <= len(r.Prs) / 2 {
			break
		}
	}
	newCommit--
	term, _ := r.RaftLog.Term(newCommit)
	if newCommit == r.RaftLog.committed || term < r.Term {
		return false
	}
	r.RaftLog.SetCommited(newCommit)
	return true
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resetElapsed(&r.electionElapsed, &r.ticks, r.electionTimeout, true)
	r.sendMessage(m.From, pb.MessageType_MsgHeartbeatResponse, pb.Message{Commit: r.RaftLog.committed})
}

func (r *Raft) handleRequestVote(m pb.Message) {
	upToDate := func(term uint64, index uint64) bool {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		return term > lastTerm || term == lastTerm && index >= lastIndex
	}
	reject := r.Vote != m.From && (m.Term < r.Term || r.Vote != 0 || !upToDate(m.LogTerm, m.Index))
	r.sendMessage(m.From, pb.MessageType_MsgRequestVoteResponse, pb.Message{
		Reject: reject,
	})
	r.log("%d from %v reject=%v vote=%v logTerm=%v index=%d lastIndex=%v Vote=%v",
	 r.id, m.From, reject, r.Vote, m.LogTerm, m.Index, r.RaftLog.LastIndex(), r.Vote)
	if !reject {
		r.readyUpdated = true
		r.Vote = m.From
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.grantVotes[m.From] = !m.Reject
	r.log("%d reject=%v from %v grantVotes=%v, Prs=%v",
	 r.id, m.Reject, m.From, r.grantVotes, r.getPeersFromPrs())
	r.checkVote()
}

func (r *Raft) GetSendMsg() []pb.Message {
	type msgkey = struct {
		to uint64
		msgType pb.MessageType
	}
	msgMap := make(map[msgkey]*pb.Message)
	ret := make([]pb.Message, 0)
	for _, msg := range r.msgs {
		if msg.MsgType == pb.MessageType_MsgSnapshot {
			msgMap[msgkey{msg.To, msg.MsgType}] = &msg
		} else {
			ret = append(ret, msg)
		}
	}
	for _, msg := range msgMap {
		ret = append(ret, *msg)
	}
	if len(ret) == 0 {
		return nil
	}
	//r.log("%d msgs=%+v\n filtermsg=%+v\n len=%d:%d", r.id, r.msgs, ret, len(r.msgs), len(ret))
	return ret
}

func (r *Raft) HandleAdvance(rd Ready) {
	r.readyUpdated = false
	r.RaftLog.readyUpdated = false
	r.msgs = make([]pb.Message, 0)
	r.RaftLog.CommitSubmited(&rd.CommittedEntries)
	if len(rd.Entries) > 0 {
		r.RaftLog.SetStabled(rd.Entries[len(rd.Entries) - 1].Index)
	}
	if rd.SoftState != nil {
		r.SoftState = rd.SoftState
	}
	if !reflect.DeepEqual(rd.HardState, pb.HardState{}) {
		r.HardState = rd.HardState
	}
	// snapshot has applied
	r.RaftLog.pendingSnapshot = nil
	r.RaftLog.maybeCompact(r.id)
}

func (r *Raft) ReadyUpdated() bool {
	return r.readyUpdated || r.RaftLog.readyUpdated
}


func (r *Raft) newSoftState() *SoftState {
	return &SoftState{
		Lead: r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) newHardState() pb.HardState {
	return pb.HardState{
		Term: r.Term,
		Vote: r.Vote,
		Commit: r.Commit,
	}
}

func (r *Raft) GetReadyState() (*SoftState, pb.HardState) {
	soft := r.newSoftState()
	hard := r.newHardState()
	if reflect.DeepEqual(soft, r.SoftState) {
		soft = nil
	}
	if reflect.DeepEqual(hard, r.HardState) {
		hard = pb.HardState{}
	}
	return soft, hard
}

func (r *Raft) GetID() uint64 {
	return r.id
}


func (r *Raft) installSnapshot(snap *pb.Snapshot) {
	r.log("%d installSnapshot=%+v", r.id, snap)
	r.RaftLog.InstallSnap(snap)
	peers := snap.Metadata.ConfState.Nodes
	r.initPrs(peers, true)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	//reject
	if m.Term < r.Term {
		// let leader update term and become follower
		r.sendMessage(m.From, pb.MessageType_MsgAppendResponse, pb.Message{
			Reject: true,
			Term: r.Term,
		})
		return
	}
	r.installSnapshot(m.GetSnapshot())
	r.log("after installSnapshot %d raftLog=%+v", r.id, r.RaftLog)
	// we nned told leader that snapshot install finish
	// and append following entries if any
	r.sendMessage(m.From, pb.MessageType_MsgAppendResponse, pb.Message{
		//LogTerm: m.Snapshot.Metadata.Index,
		Index: m.Snapshot.Metadata.Index,
		Reject: false,
	})
}

// check transferee 's log is up to date and return result.
func (r *Raft) checkTransferee() bool {
	if r.leadTransferee == 0 {
		return false
	}
	p := r.Prs[r.leadTransferee]
	if p.Match < r.RaftLog.LastIndex() {
		r.sendAppend(r.leadTransferee)
		return false
	} else {
		r.sendMessage(r.leadTransferee, pb.MessageType_MsgTimeoutNow, pb.Message{})
		r.leadTransferee = 0
		return true
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.log("%d add node %d", r.id, id)
	r.Prs[id] = r.newProgress(id)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	r.log("%d rm node %d", r.id, id)
	delete(r.Prs, id)
	switch r.State {
	case StateFollower:
		if r.Vote == id {
			r.Vote = 0
		}
	case StateCandidate:
		delete(r.grantVotes, id)
		r.checkVote()
	case StateLeader:
		r.checkUpdateCommit()
	}
}
