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

// 提供 (StateType).String() 方法
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
	Match, Next uint64
}

type Raft struct {
	id uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster.
	peers []uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	orgElectionTimeout int

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, _ := c.Storage.InitialState()

	return &Raft{
		id:                 c.ID,
		peers:              c.peers,
		Term:               hardState.Term,
		Vote:               hardState.Vote,
		RaftLog:            newLog(c.Storage),
		Prs:                make(map[uint64]*Progress),
		State:              StateFollower,
		votes:              make(map[uint64]bool),
		msgs:               make([]pb.Message, 0),
		Lead:               None,
		heartbeatTimeout:   c.HeartbeatTick,
		electionTimeout:    c.ElectionTick,
		orgElectionTimeout: c.ElectionTick,
		heartbeatElapsed:   0,
		electionElapsed:    0,
		leadTransferee:     0, // (3A)
		PendingConfIndex:   0, // (3A)
	}
}

// 广播一次Append
func (r *Raft) bcastAppend() bool {
	for _, peer := range r.peers {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	return false
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 根据Progress来确定要发送哪些Entries
	prevIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil {
		log.Errorf("get prevLogTerm false at sendAppend()")
		return false
	}

	// 准备 []*Entry
	entries := r.RaftLog.entries[prevIndex+1-r.RaftLog.offset:]
	entriesPoints := make([]*pb.Entry, len(entries))
	for i := range entries {
		entriesPoints[i] = &entries[i]
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: entriesPoints,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

func (r *Raft) bcastHeatbeat() {
	for _, peer := range r.peers {
		if peer != r.id {
			r.sendHeartbeat(peer) // 发送心跳
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// commitIndex不应大于matchIndex
	pr, ok := r.Prs[to]
	var commit uint64
	if ok {
		commit = min(r.RaftLog.committed, pr.Match)
	} else {
		commit = r.RaftLog.committed
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
		// TODO 验证心跳消息是否需要包含log信息
	}
	r.msgs = append(r.msgs, msg)
}

// 通过找match的过半的最大值来推进Commit
// note：Leader只能确认Commit自己Term的条目
func (r *Raft) tryCommit() {
	for i := r.RaftLog.LastIndex(); i > 0; i-- {
		count := 0
		for _, pr := range r.Prs {
			if pr.Match >= i {
				count++
			}
		}
		if count > len(r.peers)/2 {
			term, _ := r.RaftLog.Term(i)
			if term == r.Term && r.RaftLog.committed != i {
				// 只能commit自己的term
				r.RaftLog.committed = i
				r.bcastAppend() // TODO 优化： 只向需要推进commit的peer发送
			}
			break
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// note：调用tick()表示进行一次逻辑时钟
	// interval := rand.Intn(10) + 20 // 生成20-30内的随机数
	// for tick := range time.Tick(time.Duration(interval) * time.Millisecond) {
	// 	_ = tick // 处理unused报错
	// }
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 发送MessageType_MsgBeat 给自己
			msg := pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			}
			r.Step(msg) // 直接传递给Step()
		}
	case StateCandidate, StateFollower:
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout {
			// 一次选举超时后会重置一个随机的选举超时时间
			r.electionElapsed = 0
			r.electionTimeout = rand.Intn(2*r.orgElectionTimeout) + r.orgElectionTimeout
			// 发送MessageType_MsgHup 给自己
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			}
			// r.msgs = append(r.msgs, msg) // 传给msgs，而不是直接给Step()
			r.Step(msg) // 直接传递给Step()
		}
	}
}

// becomeFollower transform this peer's state to Follower
// note: 只修改状态 不涉及动作
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.Prs = make(map[uint64]*Progress)
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
// note: 只修改状态 不涉及动作
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// 在每次转换为Candidate时 Term++
	r.Term++

	//
	r.Vote = None
	r.Prs = make(map[uint64]*Progress)
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).

	// // 直接append一个空条目

	// entry := make([]pb.Entry, 1)
	// r.RaftLog.appendEntry(entry)

	//
	r.Vote = None
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range r.peers {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}

	r.State = StateLeader
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	// NOTE: Leader should propose a noop entry on its term

	msg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: append([]*pb.Entry{}, &pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Data:      nil}),
	}
	r.Step(msg)

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// 处理message的Term。
	// if m.Term < r.Term {
	// 	// 直接忽略Term比自己小的Msg （or 需要根据Msg类型不同来响应Reject Msg）
	// 	return nil
	// } else if m.Term > r.Term {
	// 	// 如果接受到的RPC请求或响应中，Term大于自己的，则更新Term。并切换为跟随者
	// 	r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	// } // note: 当candidater收到Term相等的心跳或Append时，也会转换为fllower

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleInternalHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgBeat:
		r.handleInternalBeat(m)
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handelAppendResponse(m)
	}

	return nil
}

func (r *Raft) handleInternalHup(m pb.Message) {
	// campaign
	switch r.State {

	case StateLeader:
	// 已经是Leader， 不用再次选举
	case StateFollower, StateCandidate:

		r.becomeCandidate()

		// 开始新的一轮选举

		// 处理单机的情况。不用发Msg
		if len(r.peers) == 1 {
			r.becomeLeader()
		}

		// 投票给自己
		r.Vote = r.id
		r.votes[r.id] = true

		logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			log.Errorf("get lastLogTerm false at Hup")
		}
		// 向每个peer发送请求投票 除了自己
		for _, peer := range r.peers {
			if peer != r.id {
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      peer,
					From:    r.id,
					Term:    r.Term,
					LogTerm: logTerm,
					Index:   r.RaftLog.LastIndex(),
				}
				r.msgs = append(r.msgs, msg)
			}
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// 收到投票请求
	// 处理Term
	if m.Term < r.Term {
		// 拒绝 Term比自己小 回复后退出函数
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	} else if m.Term > r.Term {
		// Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, None) // note: Term更高的请求投票，此时不知道leader
	}

	switch r.State {
	case StateFollower:
		// 根据规则投票
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		// note: 一个Term内只会投一次票
		if (r.Vote == None || r.Vote == m.From) && ((lastTerm < m.LogTerm) || (lastTerm == m.LogTerm && lastIndex <= m.Index)) {
			// 同意投票
			r.Vote = m.From
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, msg)
		} else {
			// 拒绝投票
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
		}
	case StateCandidate, StateLeader:
		// 拒绝投票
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {

	if m.Term < r.Term {
		// 拒绝 Term比自己小 回复后退出函数
	} else if m.Term > r.Term {
		// Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	}

	if r.State == StateCandidate {
		// 统计票数
		r.votes[m.From] = !m.Reject
		voteCount := 0
		rejectCount := 0

		for _, vote := range r.votes {
			if vote {
				if voteCount++; voteCount > (len(r.peers) / 2) {
					r.becomeLeader()
				}
			} else {
				if rejectCount++; rejectCount > (len(r.peers) / 2) {
					r.becomeFollower(r.Term, None)
				}
			}
		}
	}
}

func (r *Raft) handleInternalBeat(m pb.Message) {
	// 不考虑收到更小的Term
	switch r.State {
	case StateLeader:
		// 向每个peers发送心跳
		r.bcastHeatbeat()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// 根据Term转换角色等
	if m.Term < r.Term {
		// 失败回应
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	} else if m.Term > r.Term {
		// 如果接受到的RPC请求或响应中，Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	} else {
		// Term相等
		if r.State == StateCandidate {
			// 当candidater收到Term相等的心跳或Append时，也会转换为follower
			r.becomeFollower(m.Term, m.From)
		} else if r.State == StateFollower {
			// 当Follower收到相等的心跳或Appdend时，会修改lead
			r.Lead = m.From
		}
	}

	// 根据转换后的角色响应Append
	// append
	term, err := r.RaftLog.Term(m.Index) // Index不存在则err
	if err == nil && term == m.LogTerm {
		// println("匹配上")
		// 匹配上

		// 转换一下
		entries := make([]pb.Entry, len(m.Entries))
		for i := range entries {
			entries[i] = *m.Entries[i]
		}
		r.RaftLog.appendEntry(entries)

		// 推进commit
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(min(m.Commit, m.Index+uint64(len(m.Entries))), r.RaftLog.LastIndex())
		}
		// 回复
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Commit:  r.RaftLog.committed,
			Reject:  false,
		}
		r.msgs = append(r.msgs, msg)
	} else if err == nil && term != m.LogTerm {
		// index够长，但term不对
		// println("index够长，但term不对")
		// // 删除
		// offset := m.Index - r.RaftLog.entries[0].Index - 1
		// if r.RaftLog.stabled >= offset {
		// 	r.RaftLog.stabled = offset - 1
		// }
		// r.RaftLog.entries = append([]pb.Entry{}, r.RaftLog.entries[:offset]...)

		// 回复
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		// index不够长
		// println("index不够长")
		// 回复
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) handelAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		// 直接忽略Term比自己小的Msg （or 需要根据Msg类型不同来响应Reject Msg）
		return
	} else if m.Term > r.Term {
		// 如果接受到的RPC请求或响应中，Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	} else {
		// Term相等
		switch r.State {
		case StateLeader:
			if !m.Reject {
				// 成功append
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = r.RaftLog.LastIndex() + 1 // TODO 验证怎么更新Next
				// r.Prs[m.From].Next = m.Index + 1
				// 考虑推进commit
				r.tryCommit()
			} else {
				// 失败append
				r.Prs[m.From].Next--
				r.sendAppend(m.From)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	if m.Term < r.Term {
		// 直接忽略Term比自己小的Msg （or 需要根据Msg类型不同来响应Reject Msg）
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	} else if m.Term > r.Term {
		// 如果接受到的RPC请求或响应中，Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	} else {
		// Term相等
		if r.State == StateCandidate {
			// 当candidater收到Term相等的心跳或Append时，也会转换为fllower
			r.becomeFollower(m.Term, m.From)
		} else if r.State == StateFollower {
			// 当Follower收到相等的心跳或Appdend时，会修改lead
			r.Lead = m.From
		}
	}

	// TODO 验证：根据心跳带的LeaderCommit信息，将日志条目应用到本地的状态机中
	r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())

	// 回应心跳
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Reject:  false,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		// 直接忽略Term比自己小的Msg （or 需要根据Msg类型不同来响应Reject Msg）
		return
	} else if m.Term > r.Term {
		// 如果接受到的RPC请求或响应中，Term大于自己的，则更新Term。并切换为跟随者
		r.becomeFollower(m.Term, m.From) // note: 此时记录的lead不一定是真的leader
	} else {
		// Term相等
		switch r.State {
		case StateLeader:
			if !m.Reject {
				// 一次成功的心跳来回
				if m.Index < r.RaftLog.LastIndex() {
					r.sendAppend(m.From)
				}
			}
		}
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	// 不考虑收到更小的Term

	switch r.State {
	case StateFollower:
		// 转发
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	case StateCandidate:
		// is dropped
	case StateLeader:

		if len(m.Entries) != 1 {
			log.Errorf("Leader should only Propose Entry once a time!")
		}
		// append entries to its log
		entries := make([]pb.Entry, len(m.Entries))
		for i := range entries {
			entries[i] = *m.Entries[i]
			entries[i].Term = r.Term
			entries[i].Index = r.RaftLog.LastIndex() + 1
		}
		r.RaftLog.appendEntry(entries)

		// 额外处理单机的情况
		if len(r.peers) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return
		}

		// 依据情况更新Prs.Index[]
		for id, pr := range r.Prs {
			if id == r.id {
				pr.Match = r.RaftLog.LastIndex()
				pr.Next = pr.Match + 1
			}
			if pr.Next == r.RaftLog.LastIndex()-1 {
				pr.Next = r.RaftLog.LastIndex()
			}
		}

		// 发起给其他服务器
		r.bcastAppend()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
