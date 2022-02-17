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
	// Invariant: applied <= committed 当处理机处理速度慢时
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// note: 每次调用Ready时将unstabled的Entry写入到storage
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the index of entries[0]
	offset uint64

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

//  从storage中恢复RaftLog。
// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	lo, _ := storage.FirstIndex() // note:当entry长度为1时，Last会返回0，first会返回1
	hi, _ := storage.LastIndex()

	// entries, _ := storage.Entries(lo, hi+1) //TODO 是否需要Copy, 处理为了Copy
	temp_entries, _ := storage.Entries(lo, hi+1)
	new_entries := make([]pb.Entry, len(temp_entries))
	copy(new_entries, temp_entries)

	pendingSnapshot, _ := storage.Snapshot()
	return &RaftLog{
		storage: storage,
		//
		committed:       hardState.Commit,
		applied:         lo - 1, // Note: Initialize our committed and applied pointers to the time of the last compaction.
		stabled:         hi,
		entries:         new_entries,
		offset:          lo,
		pendingSnapshot: &pendingSnapshot,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// entries, _ := l.EntriesBegin(l.stabled + 1) // l.stabled的更新在外部
	return l.entries[l.stabled+1-l.offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// entries, _ := l.Entries(l.applied+1, l.committed+1)
	return l.entries[l.applied+1-l.offset : l.committed+1-l.offset]
}

// append entries to its log
func (l *RaftLog) appendEntry(entries []pb.Entry) error {
	// Your Code Here (2A).
	if len(entries) == 0 {
		return nil
	}

	if len(l.entries) == 0 {
		l.entries = append(l.entries, entries...)
	}

	// append之后应该的First和Last
	first := l.entries[0].Index
	last := entries[0].Index + uint64(len(entries)) - 1

	if last < first {
		return nil
	}
	// 截断
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 将entries放入l.entries
	if l.LastIndex()+1 < entries[0].Index {
		// 接不上
		log.Panicf("missing log entry [last: %d, append at: %d]",
			l.entries[len(l.entries)-1].Index, entries[0].Index)
	} else if l.LastIndex()+1 == entries[0].Index {
		// 刚好接上
		l.entries = append(l.entries, entries...)
	} else {
		// 从中间开始，需要逐个比对
		for i := range entries {
			if entries[i].Index <= l.LastIndex() {
				// 存在
				loc := entries[i].Index - l.entries[0].Index

				// 匹配不上 需要删除原有的i及i之后 并 append 结束循环
				if entries[i].Term != l.entries[loc].Term {
					// 删除需修改stabled
					if l.stabled >= entries[i].Index {
						l.stabled = entries[i].Index - 1
					}
					l.entries = append([]pb.Entry{}, l.entries[:loc]...)
					l.entries = append(l.entries, entries...)
					break
				}
			} else {
				// 到不存在了
				l.entries = append(l.entries, entries[i:]...)
			}
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0 // TODO 验证entries长度为0时是否应该返回compact的最后一个Index
	} else {
		return l.entries[len(l.entries)-1].Index
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 找到Index对应的Entry 错误使用 errors.New("")

	if i == 0 {
		return 0, nil
	}
	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}

	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

// func (l *RaftLog) EntriesBegin(begin uint64) ([]pb.Entry, error) {

// 	var offset uint64
// 	if len(l.entries) == 0 {
// 		offset = 1
// 	} else {
// 		offset = l.entries[0].Index
// 	}

// 	if begin < offset {
// 		return nil, ErrCompacted
// 	}
// 	ents := l.entries[begin-offset:]
// 	return ents, nil
// }

// func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {

// 	var offset uint64
// 	if len(l.entries) == 0 {
// 		offset = 1
// 	} else {
// 		offset = l.entries[0].Index
// 	}
// 	if lo < offset {
// 		return nil, ErrCompacted
// 	}

// 	if hi > l.LastIndex()+1 {
// 		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, l.LastIndex())
// 	}

// 	ents := l.entries[lo-offset : hi-offset] // note： ents的index 和 ms.ents[0].Index
// 	// if len(l.entries) == 0 && len(ents) != 0 {
// 	// 	// only contains dummy entries.
// 	// 	return nil, ErrUnavailable
// 	// }
// 	return ents, nil
// }
