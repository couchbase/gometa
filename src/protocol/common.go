package protocol

import (
	"common"
)

/////////////////////////////////////////////////////////////////////////////
// PeerKind
/////////////////////////////////////////////////////////////////////////////

type PeerRole byte

const (
	LEADER PeerRole = iota
	FOLLOWER
	OBSERVER
)

/////////////////////////////////////////////////////////////////////////////
// PeerStatus
/////////////////////////////////////////////////////////////////////////////

type PeerStatus byte

const (
	ELECTING PeerStatus = iota
	LEADING
	FOLLOWING
	OBSERVING
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
	
	GetBootstrapLastLoggedTxid() common.Txnid

	GetLastCommittedTxid() (common.Txnid, error)
	
	GetBootstrapLastCommittedTxid() common.Txnid

	GetStatus() PeerStatus

	// Current Epoch is set during leader/followr discovery phase.
	// It is the current epoch (term) of the leader.
	GetCurrentEpoch() (uint32, error)
	
	GetBootstrapCurrentEpoch() uint32

	// This is the Epoch that leader/follower agrees during discovery/sync phase.
	GetAcceptedEpoch() (uint32, error)
	
	GetBootstrapAcceptedEpoch() uint32

	//
	// The following API are used during discovery/sync
	//

	GetCommitedEntries(txid uint64) (<- chan LogEntryMsg, <- chan error, error)

	AppendLog(txid uint64, op uint32, key string, content []byte) error

	// Set new accepted epoch as well as creating new txnid
	NotifyNewAcceptedEpoch(uint32)

	NotifyNewCurrentEpoch(uint32)
	
	NotifyNewLastCommittedTxid(uint64)

	//
	// The following API are used during normal execution
	//
	GetFollowerId() string
	
	LogProposal(proposal ProposalMsg) error

	Commit(proposal ProposalMsg) error
}

/////////////////////////////////////////////////////////////////////////////
// MsgFactory
/////////////////////////////////////////////////////////////////////////////

type MsgFactory interface {
	CreateProposal(txnid uint64, fid string, reqId uint64, op uint32, key string, content []byte) ProposalMsg

	CreateAccept(txnid uint64, fid string) AcceptMsg

	CreateCommit(txnid uint64) CommitMsg

	CreateVote(round uint64, status uint32, epoch uint32, cndId string, cndTxnId uint64) VoteMsg

	CreateFollowerInfo(epoch uint32, fid string) FollowerInfoMsg

	CreateEpochAck(lastLoggedTxid uint64, epoch uint32) EpochAckMsg

	CreateLeaderInfo(epoch uint32) LeaderInfoMsg

	CreateNewLeader(epoch uint32) NewLeaderMsg

	CreateNewLeaderAck() NewLeaderAckMsg

	CreateLogEntry(txnid uint64, opCode uint32, key string, content []byte) LogEntryMsg

	CreateRequest(id uint64, opCode uint32, key string, content []byte) RequestMsg
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

type RequestMsg interface {
	common.Packet
	GetReqId() uint64
	GetOpCode() uint32
	GetKey() string
	GetContent() []byte
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
	GetCndTxnId() uint64
}

/////////////////////////////////////////////////////////////////////////////
// Message for discovery
/////////////////////////////////////////////////////////////////////////////

type FollowerInfoMsg interface {
	common.Packet
	GetAcceptedEpoch() uint32
	GetFid() string 
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
	GetEpoch() uint32
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
