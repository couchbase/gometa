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
	// The following API are used during election
	//	
	GetLastLoggedTxid() common.Txnid
	
	GetStatus() PeerStatus
	
	// Current Epoch is set during leader/followr discovery phase.
	// It is the current epoch (term) of the leader.
	GetCurrentEpoch() uint32

	// This is the Epoch that leader/follower agrees during discovery/sync phase. 
	GetAcceptedEpoch() uint32
	
	//
	// The following API are used during discovery/sync 
	//	
	
	// Set new accepted epoch as well as creating new txnid
	NotifyNewAcceptedEpoch(uint32) 
	
	NotifyNewCurrentEpoch(uint32) 
	
	//
	// The following API are used during normal execution 
	//	
	Commit(proposal ProposalMsg) error
}

/////////////////////////////////////////////////////////////////////////////
// MsgFactory 
/////////////////////////////////////////////////////////////////////////////

type MsgFactory interface {

	CreateProposal(txnid uint64, fid string, op uint32, key string, content []byte) ProposalMsg 
	
	CreateAccept(txnid uint64, fid string) AcceptMsg 
										
	CreateCommit(txnid uint64) CommitMsg 
										
	// TODO : Cleanup
	CreateBallot(id uint64) BallotMsg 
	
	CreateVote(round uint64, status uint32, epoch uint32, cndId string, cndTxnId uint64) VoteMsg 
	
	CreateFollowerInfo(epoch uint32) FollowerInfoMsg 
	
	CreateEpochAck(epoch uint32) EpochAckMsg 
	
	CreateLeaderInfo(epoch uint32) LeaderInfoMsg 
	
	CreateNewLeader() NewLeaderMsg 
	
	CreateNewLeaderAck() NewLeaderAckMsg 
	
	CreateLogEntry(opCode uint32, key string, content []byte) LogEntryMsg
}

/////////////////////////////////////////////////////////////////////////////
// Message for normal execution
/////////////////////////////////////////////////////////////////////////////

type ProposalMsg interface {
	common.Packet
    GetTxnid() uint64
    GetFid() string
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

// TODO : Cleanup
type BallotMsg interface {
	common.Packet
 	GetId() uint64 
}

/////////////////////////////////////////////////////////////////////////////
// Message for discovery 
/////////////////////////////////////////////////////////////////////////////

type FollowerInfoMsg interface {
	common.Packet
	GetAcceptedEpoch() uint32
}

type LeaderInfoMsg interface {
	common.Packet
	GetAcceptedEpoch() uint32
}

type EpochAckMsg interface {
	common.Packet
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
	GetOpCode() uint32 
    GetKey() string 
    GetContent() []byte 
}