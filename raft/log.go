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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
	PanicErr(err)
	return &RaftLog {
		storage: storage,
		applied: 0,
		committed: hardState.Commit,
		entries: entries,
		stabled: lastIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

const (
	EqLess = 0
	EqGreater = 1
)

func (l *RaftLog) findPos(entIndex uint64, option int) int {
	for index, entry := range l.entries {
		if entry.Index == entIndex {
			return index
		}
		if entry.Index > entIndex {
			if option == EqLess {
				return index - 1
			} else {
				return index
			}
		}
	}
	if option == EqLess {
		return len(l.entries) - 1
	} else {
		return len(l.entries)
	}
}

func (l *RaftLog) EntryMatch(entTerm, entIndex uint64) bool {
	if entIndex == 0 && entTerm == 0 {
		return true
	}
	index := l.findPos(entIndex, EqGreater)
	if (index >= len(l.entries)) {
		return false
	}
	return l.entries[index].Index == entIndex && l.entries[index].Term == entTerm
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ret := make([]pb.Entry, 0)
	for _, ent := range l.GetEntries(l.stabled + 1, l.LastIndex() + 1) {
		ret = append(ret, *ent)
	}
	return ret
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ret := make([]pb.Entry, 0)
	for _, ent := range l.GetEntries(l.applied + 1, l.committed + 1) {
		ret = append(ret, *ent)
	}
	return ret
}

func (l *RaftLog) Append(prevIndex uint64, ents ...*pb.Entry) {
	index := l.findPos(prevIndex + 1, EqGreater)
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

	stabledIndex := l.findPos(l.stabled, EqGreater)
	l.entries = l.entries[:index]
	if stabledIndex >= index {
		l.stabled = l.LastIndex()
	}
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
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

func (l *RaftLog) GetEntries(startIndex uint64, upperIndex uint64) []*pb.Entry {
	if (startIndex > upperIndex) {
		return []*pb.Entry{}
	}
	lower := l.findPos(startIndex, EqGreater)
	upper := l.findPos(upperIndex, EqGreater)
	ret := make([]*pb.Entry, upper - lower)
	for i := lower; i < upper; i++ {
		ret[i - lower] = &l.entries[i]
	}
	return ret
}

func (l *RaftLog) GetUncommit() []pb.Entry {
	index := l.findPos(l.committed + 1, EqGreater)
	return l.entries[index:]
}

func (l *RaftLog) GetNewCommit() []pb.Entry {
	return CopyEntsFromPtr(l.GetEntries(l.applied + 1, l.committed + 1))
}

func (l *RaftLog) CommitSubmited(ents *[]pb.Entry) {
	if (len(*ents) == 0) {
		return
	}
	l.applied = max(l.applied, (*ents)[len(*ents) - 1].Index)
	l.readyUpdated = false
}

func (l *RaftLog) SetStabled(stabled uint64) {
	l.stabled = max(l.stabled, stabled)
	index := l.findPos(l.stabled, EqLess)
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
		return 0
	}
	return l.entries[len(l.entries) - 1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if (i == 0) {
		return 0, nil
	}
	for _, e := range l.entries {
		if e.Index == i {
			return e.Term, nil
		}
	}
	return 0, ErrUnavailable
}
