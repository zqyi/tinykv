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
	"fmt"
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

	// // peers contains the IDs of all nodes (including self) in the raft cluster.
	// peers []uint64

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
	// In a short, 记录上次ConfChange的Index，接下来的ConfChange必须在该Index已经被Apply之后Propose
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	raft := &Raft{
		id:                 c.ID,
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
		// Prs:                make(map[uint64]*Progress),
	}

	// rand.Seed(time.Now().Unix())
	raft.electionTimeout = rand.Intn(raft.orgElectionTimeout) + raft.orgElectionTimeout

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	// note: peer信息是存在raft.Prs里的 2A是存在了peer里 需要将peer取缔，以防止后续BUG
	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{
			Next:  lastIndex + 1,
			Match: 0}
	}

	return raft
}

// 广播一次Append
func (r *Raft) bcastAppend() bool {

	for peer := range r.Prs {
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
	prevIndex := uint64(0)
	if r.Prs[to].Next > 0 {
		prevIndex = r.Prs[to].Next - 1
	}
	prevLogTerm, err := r.RaftLog.Term(prevIndex)
	// if err != nil && prevIndex < r.RaftLog.FirstIndex()-1 {
	// 	panic(err)
	// }
	if err == ErrCompacted {
		// 发送snapshot
		r.sendSnapshot(to)
		return true
	} else if err != nil {
		// ErrUnavailable
		println(len(r.RaftLog.entries), prevIndex, r.RaftLog.FirstIndex())
		log.Errorf("get prevLogTerm false when sendAppend() %s get index %d", err, prevIndex)
		panic("get prevLogTerm false when sendAppend()")
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
	return true
}

func (r *Raft) sendSnapshot(to uint64) bool {
	// Log.pendingSnapsohot不为空表示当前节点有未应用的Snapshot
	var snapshot pb.Snapshot
	var err error = nil
	if IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot, err = r.RaftLog.storage.Snapshot()
	} else {
		snapshot = *r.RaftLog.pendingSnapshot
	}
	if err != nil {
		return false
	}

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) bcastHeatbeat() {
	for peer := range r.Prs {
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
		if count > len(r.Prs)/2 {
			term, _ := r.RaftLog.Term(i)
			if term == r.Term && r.RaftLog.committed != i {
				// 只能commit自己的term
				r.RaftLog.committed = i
				// log.Errorf("leader %d commit %d", r.id, r.RaftLog.committed)
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
		if r.heartbeatElapsed >= r.heartbeatTimeout {
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
		if r.electionElapsed >= r.electionTimeout {
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
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = 0

	// r.Prs = make(map[uint64]*Progress)

}

// becomeCandidate transform this peer's state to candidate
// note: 只修改状态 不涉及动作
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Errorf("node %d becomeCandidate term %d with index %d", r.id, r.Term, r.RaftLog.LastIndex())

	// 在每次转换为Candidate时 Term++
	r.Term++

	//
	r.Vote = None
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = 0

	// r.Prs = make(map[uint64]*Progress)

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	log.Errorf("node %d becomeLeader at term %d with index %d", r.id, r.Term, r.RaftLog.LastIndex())
	// Your Code Here (2A).

	// // 直接append一个空条目

	// entry := make([]pb.Entry, 1)
	// r.RaftLog.appendEntry(entry)

	//
	r.Vote = None
	// r.Prs = make(map[uint64]*Progress)
	for peer, prs := range r.Prs {
		if peer == r.id {
			prs.Match = r.RaftLog.LastIndex()
			prs.Next = r.RaftLog.LastIndex() + 1
		} else {

			prs.Match = 0
			prs.Next = r.RaftLog.LastIndex() + 1
		}
	}

	r.State = StateLeader
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = 0
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
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleMsgTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeoutNow(m)
	default:
		panic("msg that dont support")
	}

	// note: 这里返回的err会在peerMsgHandler.proposeRaftCommand()捕获
	// 考虑是否需要根据执行结果返回err
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
		if len(r.Prs) == 1 {
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
		msgs := []pb.Message{}
		for peer := range r.Prs {
			if peer != r.id {
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      peer,
					From:    r.id,
					Term:    r.Term,
					LogTerm: logTerm,
					Index:   r.RaftLog.LastIndex(),
				}
				msgs = append(msgs, msg)
			}
		}
		// log.Infof("node %d begin Hup at term %d", r.id, r.Term)
		r.msgs = append(r.msgs, msgs...)
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
		// log.Infof("%s %d reject %d as term, node term: %d vs %d, lastIndex: %d vs %d, voto: %dn", r.State.String(), r.id, m.From, m.Term, m.Term, r.RaftLog.LastIndex(), m.Index, r.Vote)
		return
	} else if m.Term > r.Term {
		// Term大于自己的，则更新Term。并切换为跟随者
		// r.becomeFollower 但是 不重置选举超时
		r.Term = m.Term
		r.Vote = None
		r.State = StateFollower
		r.votes = make(map[uint64]bool)
		r.Lead = None
		// r.heartbeatElapsed = 0
		// r.electionElapsed = 0
	}

	// TODO 虽然term更大，但日志不新，则不重置计时

	switch r.State {
	case StateFollower:
		// 根据规则投票
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			panic("get term faild when handleRequestVote")
		}
		// note: 一个Term内只会投一次票
		if (r.Vote == None || r.Vote == m.From) && ((lastTerm < m.LogTerm) || (lastTerm == m.LogTerm && lastIndex <= m.Index)) {
			// 同意投票
			// 同意投票时重置选举超时
			r.heartbeatElapsed = 0
			r.electionElapsed = 0
			//
			r.Vote = m.From
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			}
			// log.Infof("%s %d vote %d ,node term: %d vs %d, lastIndex: %d vs %d, lastTerm %d vs %d, voto: %dn", r.State.String(), r.id, m.From, m.Term, m.Term, r.RaftLog.LastIndex(), m.Index, lastTerm, m.LogTerm, r.Vote)
			r.msgs = append(r.msgs, msg)
		} else {
			// 拒绝投票
			// // 因日志而拒绝投票后随机选择新的选举超时
			// if (lastTerm < m.LogTerm) || (lastTerm == m.LogTerm && lastIndex <= m.Index) {
			// 	r.electionTimeout = rand.Intn(r.orgElectionTimeout) + 1
			// }
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			}
			// log.Infof("%s %d reject %d as log ,node term: %d vs %d, lastIndex: %d vs %d, lastTerm %d vs %d, voto: %dn", r.State.String(), r.id, m.From, m.Term, m.Term, r.RaftLog.LastIndex(), m.Index, lastTerm, m.LogTerm, r.Vote)
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
		// log.Infof("%s %d reject %d as not fllower,node term: %d vs %d, lastIndex: %d vs %d, voto: %dn", r.State.String(), r.id, m.From, m.Term, m.Term, r.RaftLog.LastIndex(), m.Index, r.Vote)
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
		r.electionElapsed = 0
		// 统计票数
		r.votes[m.From] = !m.Reject
		voteCount := 0
		rejectCount := 0

		for _, vote := range r.votes {
			if vote {
				if voteCount++; voteCount > (len(r.Prs) / 2) {
					r.becomeLeader()
				}
			} else {
				if rejectCount++; rejectCount > (len(r.Prs) / 2) {
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
			// Index:   m.Index + 1, // TODO 验证
			Reject: true,
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
			r.electionElapsed = 0
		}
	}

	// 根据转换后的角色响应Append
	// append
	term, err := r.RaftLog.Term(m.Index) // Index不存在则err
	if err == nil && term == m.LogTerm {
		// 匹配上

		// 转换一下
		entries := make([]pb.Entry, len(m.Entries))
		for i := range entries {
			entries[i] = *m.Entries[i]
		}
		r.RaftLog.appendEntry(entries)
		// log.Errorf("%d append entry from %d to %d", r.id, m.Index, r.RaftLog.LastIndex())

		// 推进commit
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(min(m.Commit, m.Index+uint64(len(m.Entries))), r.RaftLog.LastIndex())
		}
		// log.Errorf("%d commit %d", r.id, r.RaftLog.committed)

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
			Index:   m.Index,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		// index不够长
		// 回复
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   m.Index,
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

				//transferLeader判断
				if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
					r.transferLeader(m.From)
					return
				}
				// 考虑推进commit
				r.tryCommit()
			} else {
				// 失败append m.Index的entry Term 匹配失败
				// 当 m.Index >= Next时 不需要发送append
				if r.Prs[m.From].Next > m.Index {
					// 当 m.Index >= Next时 不需要发送append
					r.Prs[m.From].Next = m.Index
					r.sendAppend(m.From)
				}
			}
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	metadata := m.Snapshot.Metadata
	// 验证Snapshot是否过时
	if m.Term < r.Term || metadata.Index <= r.RaftLog.committed {
		log.Info(fmt.Sprintf("%d [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, metadata.Index, metadata.Term))
		return
	}

	log.Info(fmt.Sprintf("%d [commit: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, metadata.Index, metadata.Term))

	r.becomeFollower(m.Term, m.From)

	// 修改entries
	if len(r.RaftLog.entries) > 0 {
		if metadata.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = []pb.Entry{}
		} else if metadata.Index >= r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.entries[metadata.Index-r.RaftLog.FirstIndex():]
		}
	}

	r.RaftLog.applied = metadata.Index
	r.RaftLog.committed = metadata.Index
	r.RaftLog.stabled = metadata.Index
	r.RaftLog.offset = metadata.Index + 1

	// 修改Prs
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range metadata.ConfState.Nodes {
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[peer] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot

	// 用AppendResponse回复
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
			r.electionElapsed = 0
		}
	}

	// TODO 验证：根据心跳带的LeaderCommit信息，将日志条目应用到本地的状态机中
	r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	// log.Errorf("%d commit %d", r.id, r.RaftLog.committed)

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
		// Leader正在进行转移，停止接受propose
		if r.leadTransferee != None {
			return
		}

		// 处理 ConfChange
		if m.Entries[0].EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex <= r.RaftLog.applied {
				r.PendingConfIndex = r.RaftLog.LastIndex() + 1
			} else {
				// 如果上次ConfChange没有Apply，则不能将这次Propose
				log.Errorf("%d cant propose confchange, pendingIndex %d, applied %d, last %d", r.id, r.PendingConfIndex, r.RaftLog.applied, r.RaftLog.LastIndex())
				return
			}
		}

		// 终于能够Propose. append entries to its log
		entries := make([]pb.Entry, len(m.Entries))
		for i := range entries {
			entries[i] = *m.Entries[i]
			entries[i].Term = r.Term
			entries[i].Index = r.RaftLog.LastIndex() + 1
		}
		r.RaftLog.appendEntry(entries)

		// 额外处理单机的情况
		if len(r.Prs) == 1 {
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
		// r.PendingConfIndex = r.RaftLog.LastIndex()
		// 发起给其他服务器
		r.bcastAppend()
	}
}

func (r *Raft) handleMsgTransferLeader(m pb.Message) {
	switch r.State {
	case StateLeader:
		if _, ok := r.Prs[m.From]; !ok {
			// 被转移者不合法
			return
		}
		if m.From == r.id {
			// 已经是Leader了
			return
		}

		// 被转移者
		r.leadTransferee = m.From

		// 在AppendResponse判断是否合格
		r.sendAppend(m.From)
	case StateCandidate, StateFollower:
		// 发给了非Leader 转发
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}

}

// call by Raft.handelAppendResponse()
func (r *Raft) transferLeader(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      r.leadTransferee,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)

	r.becomeFollower(r.Term, None)
}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	// 判断自己在不在Prs中
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	// 发送MessageType_MsgHup 给自己
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		To:      r.id,
		From:    r.id,
	}
	r.Step(msg) // 直接传递给Step()
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
	progress := &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = progress
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).

	if r.id == id {
		// log.Errorf("%d remove self at Raft.removeNode()", r.id)
		r.Prs = make(map[uint64]*Progress)
		return
	}
	delete(r.Prs, id)
	delete(r.votes, id)
	r.tryCommit()
}
