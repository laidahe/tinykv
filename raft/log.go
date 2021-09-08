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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	readyUpdated bool
	// save snapshot's last included Term and Index
	snapIndex, snapTerm uint64
	log func(string, ...interface{}) string
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	PanicErr(err)
	firstIndex, err := storage.FirstIndex()
	PanicErr(err)
	lastIndex, err := storage.LastIndex()
	PanicErr(err)
	entries, err := storage.Entries(firstIndex, lastIndex + 1)
	log.Infof("firstIndex=%d, lastIndex=%d", firstIndex, lastIndex)
	PanicErr(err)
	var snapshot *pb.Snapshot = nil
	snapIndex := firstIndex - 1
	snapTerm, _ := storage.Term(snapIndex)
	applied := snapIndex
	// if err != nil {
	// 	snap, err := storage.Snapshot()
	// 	if err == nil {
	// 		// snapshot exist
	// 		log.Infof("recover from snap, snapIndex=%d snapTerm=%d lastIndex=%d", snapIndex, snapTerm, lastIndex)
	// 		snapshot = &snap
	// 		snapIndex, snapTerm = snap.GetMetadata().Index, snap.GetMetadata().Term
	// 		applied = snapIndex
	// 		// get entries following snapshot
	// 		entries, err = storage.Entries(snapIndex + 1, lastIndex + 1)
	// 		PanicErr(err)
	// 	} else {
	// 		entries = make([]pb.Entry, 0)
	// 	}
	// }
	return &RaftLog {
		storage: storage,
		applied: applied,
		committed: hardState.Commit,
		entries: entries,
		stabled: lastIndex,
		snapIndex: snapIndex,
		snapTerm: snapTerm,
		pendingSnapshot: snapshot,
	}
}

func newLogWithLogFunc(s Storage, log func(string, ...interface{}) string) *RaftLog {
	ret := newLog(s)
	ret.log = log
	return ret
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact(id uint64) {
	// Your Code Here (2C).
	// remove entries index <= truncatedIndex
	truncatedIndex, err := l.storage.FirstIndex()
	truncatedIndex -= 1
	if err != nil || l.snapIndex == truncatedIndex {
		return
	}
	pos := l.findPos(truncatedIndex, Eq)
	if pos == -1 {
		return
	}
	log.Infof("%d compact log <= %d", id, l.snapIndex)
	l.snapIndex, l.snapTerm = l.entries[pos].Index, l.entries[pos].Term
	l.entries = l.entries[pos + 1:]
	l.pendingSnapshot = nil
	l.readyUpdated = true
}

const (
	EqLastLess = 0
	EqFirstGreater = 1
	Eq = 2
)

func (l *RaftLog) findPos(entIndex uint64, option int) int {
	for index, entry := range l.entries {
		if entry.Index == entIndex {
			return index
		}
		if entry.Index > entIndex {
			switch option {
			case EqLastLess:
				return index - 1
			case EqFirstGreater:
				return index
			case Eq:
				return -1
			}
		}
	}

	switch option {
	case EqLastLess:
		return len(l.entries) - 1
	case EqFirstGreater:
		return len(l.entries)
	default:
		return -1
	}
}

func (l *RaftLog) EntryMatch(entTerm, entIndex uint64) bool {
	if entIndex == 0 && entTerm == 0 || entIndex == l.snapIndex && entTerm == l.snapTerm {
		return true
	}
	index := l.findPos(entIndex, EqFirstGreater)
	if (index >= len(l.entries)) {
		return false
	}
	return l.entries[index].Index == entIndex && l.entries[index].Term == entTerm
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ret := make([]pb.Entry, 0)
	for _, ent := range l.GetEntries(l.stabled + 1, l.LastIndex() + 1, false) {
		ret = append(ret, *ent)
	}
	return ret
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ret := make([]pb.Entry, 0)
	for _, ent := range l.GetEntries(l.applied + 1, l.committed + 1, false) {
		ret = append(ret, *ent)
	}
	return ret
}

func (l *RaftLog) Append(prevIndex uint64, ents ...*pb.Entry) {
	index := l.findPos(prevIndex + 1, EqFirstGreater)
	// skip exist element
	for ; len(ents) > 0 && index < len(l.entries); index++ {
		if l.entries[index].Index != ents[0].Index ||
		   l.entries[index].Term != ents[0].Term {
			break
		}
		ents = ents[1:]
	}

	if (len(ents) == 0) {
		return
	}

	stabledIndex := l.findPos(l.stabled, EqFirstGreater)
	l.entries = l.entries[:index]
	if stabledIndex >= index {
		l.stabled = l.LastIndex()
	}
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
	l.readyUpdated = true
}


func (l *RaftLog) InstallSnap(snap *pb.Snapshot) {
	if snap.Metadata.Index < l.snapIndex {
		return
	}
	l.snapIndex, l.snapTerm = snap.Metadata.Index, snap.Metadata.Term
	pos := l.findPos(l.snapIndex + 1, EqFirstGreater)
	// for the case which Follower install snapshot
	l.stabled = max(l.stabled, l.snapIndex)
	l.committed = max(l.committed, l.snapIndex)
	l.applied = max(l.applied, l.snapIndex)

	l.entries = l.entries[pos:]
	l.pendingSnapshot = snap
	l.readyUpdated = true
}

func (l *RaftLog) SetCommited(index uint64) {
	l.committed = max(l.committed, min(index, l.LastIndex()))
	if l.committed > l.applied {
		l.readyUpdated = true
	}
}

func CopyEntsFromPtr(ents []*pb.Entry) []pb.Entry {
	ret := make([]pb.Entry, 0)
	for _, ent := range ents {
		ret = append(ret, *ent)
	}
	return ret
}

func NewEntsPtr(ents []pb.Entry) []*pb.Entry {
	ret := make([]*pb.Entry, 0)
	for _, ent := range ents {
		ret = append(ret, &ent)
	}
	return ret
}

// return entries which index in (startIndex, upperIndex).
// if mustIncludeSt=true and entry which index=startIndex doesn't exist,
// return empty slice
func (l *RaftLog) GetEntries(startIndex uint64, upperIndex uint64, mustIncludeSt bool) []*pb.Entry {
	if (startIndex > upperIndex) {
		return []*pb.Entry{}
	}
	var lower int
	if !mustIncludeSt {
		lower = l.findPos(startIndex, EqFirstGreater)
	} else {
		lower = l.findPos(startIndex, Eq)
		if lower < 0 {
			return []*pb.Entry{}
		}
	}
	
	upper := l.findPos(upperIndex, EqFirstGreater)
	ret := make([]*pb.Entry, upper - lower)
	for i := lower; i < upper; i++ {
		ret[i - lower] = &l.entries[i]
	}
	return ret
}

func (l *RaftLog) GetUncommit() []pb.Entry {
	index := l.findPos(l.committed + 1, EqFirstGreater)
	return l.entries[index:]
}

func (l *RaftLog) GetNewCommit() []pb.Entry {
	return CopyEntsFromPtr(l.GetEntries(l.applied + 1, l.committed + 1, false))
}

func (l *RaftLog) CommitSubmited(ents *[]pb.Entry) {
	if (len(*ents) == 0) {
		return
	}
	last := (*ents)[len(*ents) - 1].Index
	l.applied = max(l.applied, last)
	log.Infof("commit submit last index=%d applied=%d", last, l.applied)
	l.readyUpdated = false
}

func (l *RaftLog) SetStabled(stabled uint64) {
	l.stabled = max(l.stabled, stabled)
	index := l.findPos(l.stabled, EqLastLess)
	if index < 0 {
		l.stabled = 0
	} else {
		l.stabled = l.entries[index].Index
	}
	if l.stabled == stabled {
		l.readyUpdated = false
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		// maybe install snapshot before
		return l.snapIndex
	}
	return l.entries[len(l.entries) - 1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if i == l.snapIndex {
		return l.snapTerm, nil
	}
	for _, e := range l.entries {
		if e.Index == i {
			return e.Term, nil
		}
	}
	log.Warnf("Term can't get Term %v", i)
	return 0, ErrUnavailable
}
