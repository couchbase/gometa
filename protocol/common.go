// @author Couchbase <info@couchbase.com>
// @copyright 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"github.com/couchbase/gometa/common"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// PeerKind
/////////////////////////////////////////////////////////////////////////////

type PeerRole byte

const (
	LEADER PeerRole = iota
	FOLLOWER
	WATCHER
)

/////////////////////////////////////////////////////////////////////////////
// PeerStatus
/////////////////////////////////////////////////////////////////////////////

type PeerStatus byte

const (
	ELECTING PeerStatus = iota
	LEADING
	FOLLOWING
	WATCHING
)

/////////////////////////////////////////////////////////////////////////////
// ActionHandler
/////////////////////////////////////////////////////////////////////////////

type ActionHandler interface {

	//
	// Environment API
	//
	GetEnsembleSize() uint64

	//
	// The following API are used during election
	//
	GetLastLoggedTxid() (common.Txnid, error)

	GetLastCommittedTxid() (common.Txnid, error)

	GetStatus() PeerStatus

	GetQuorumVerifier() QuorumVerifier

	// Current Epoch is set during leader/followr discovery phase.
	// It is the current epoch (term) of the leader.
	GetCurrentEpoch() (uint32, error)

	// This is the Epoch that leader/follower agrees during discovery/sync phase.
	GetAcceptedEpoch() (uint32, error)

	//
	// The following API are used during discovery/sync
	//

	GetCommitedEntries(txid1, txid2 common.Txnid) (<-chan LogEntryMsg, <-chan error, chan<- bool, error)

	LogAndCommit(txid common.Txnid, op uint32, key string, content []byte, toCommit bool) error

	// Set new accepted epoch as well as creating new txnid
	NotifyNewAcceptedEpoch(uint32) error

	NotifyNewCurrentEpoch(uint32) error

	//
	// The following API are used during normal execution
	//
	GetNextTxnId() common.Txnid

	GetFollowerId() string

	LogProposal(proposal ProposalMsg) error

	Commit(txid common.Txnid) error

	Abort(fid string, reqId uint64, err string) error

	Respond(fid string, reqId uint64, err string) error
}

/////////////////////////////////////////////////////////////////////////////
// QuorumVerifier
/////////////////////////////////////////////////////////////////////////////

type QuorumVerifier interface {
	HasQuorum(count int) bool
}

/////////////////////////////////////////////////////////////////////////////
// MsgFactory
/////////////////////////////////////////////////////////////////////////////

type MsgFactory interface {
	CreateProposal(txnid uint64, fid string, reqId uint64, op uint32, key string, content []byte) ProposalMsg

	CreateAccept(txnid uint64, fid string) AcceptMsg

	CreateCommit(txnid uint64) CommitMsg

	CreateAbort(fid string, reqId uint64, err string) AbortMsg

	CreateVote(round uint64, status uint32, epoch uint32, cndId string, cndLoggedTxnId uint64,
		cndCommittedTxnId uint64, solicit bool) VoteMsg

	CreateFollowerInfo(epoch uint32, fid string, voting bool) FollowerInfoMsg

	CreateEpochAck(lastLoggedTxid uint64, epoch uint32) EpochAckMsg

	CreateLeaderInfo(epoch uint32) LeaderInfoMsg

	CreateNewLeader(epoch uint32) NewLeaderMsg

	CreateNewLeaderAck() NewLeaderAckMsg

	CreateLogEntry(txnid uint64, opCode uint32, key string, content []byte) LogEntryMsg

	CreateRequest(id uint64, opCode uint32, key string, content []byte) RequestMsg

	CreateResponse(fid string, reqId uint64, err string) ResponseMsg
}

/////////////////////////////////////////////////////////////////////////////
// Message for normal execution
/////////////////////////////////////////////////////////////////////////////

type ProposalMsg interface {
	common.Packet
	GetTxnid() uint64
	GetFid() string
	GetReqId() uint64
	GetOpCode() uint32
	GetKey() string
	GetContent() []byte
}

type AcceptMsg interface {
	common.Packet
	GetTxnid() uint64
	GetFid() string
}

type CommitMsg interface {
	common.Packet
	GetTxnid() uint64
}

type AbortMsg interface {
	common.Packet
	GetFid() string
	GetReqId() uint64
	GetError() string
}

type RequestMsg interface {
	common.Packet
	GetReqId() uint64
	GetOpCode() uint32
	GetKey() string
	GetContent() []byte
}

type ResponseMsg interface {
	common.Packet
	GetFid() string
	GetReqId() uint64
	GetError() string
}

/////////////////////////////////////////////////////////////////////////////
// Message for master election
/////////////////////////////////////////////////////////////////////////////

type VoteMsg interface {
	common.Packet
	GetRound() uint64
	GetStatus() uint32
	GetEpoch() uint32
	GetCndId() string
	GetCndLoggedTxnId() uint64
	GetCndCommittedTxnId() uint64
	GetSolicit() bool
}

/////////////////////////////////////////////////////////////////////////////
// Message for discovery
/////////////////////////////////////////////////////////////////////////////

type FollowerInfoMsg interface {
	common.Packet
	GetAcceptedEpoch() uint32
	GetFid() string
	GetVoting() bool
}

type LeaderInfoMsg interface {
	common.Packet
	GetAcceptedEpoch() uint32
}

type EpochAckMsg interface {
	common.Packet
	GetLastLoggedTxid() uint64
	GetCurrentEpoch() uint32
}

type NewLeaderMsg interface {
	common.Packet
	GetCurrentEpoch() uint32
}

type NewLeaderAckMsg interface {
	common.Packet
}

type LogEntryMsg interface {
	common.Packet
	GetTxnid() uint64
	GetOpCode() uint32
	GetKey() string
	GetContent() []byte
}

/////////////////////////////////////////////////////////////////////////////
// Request Management
/////////////////////////////////////////////////////////////////////////////

type RequestHandle struct {
	Request   RequestMsg
	Err       error
	Mutex     sync.Mutex
	CondVar   *sync.Cond
	StartTime int64
}

type RequestMgr interface {
	GetRequestChannel() <-chan *RequestHandle
	AddPendingRequest(handle *RequestHandle)
	CleanupOnError()
}

type CustomRequestHandler interface {
	OnNewRequest(fid string, request RequestMsg)
	GetResponseChannel() <-chan common.Packet
}
