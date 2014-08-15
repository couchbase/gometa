package message

import (
	"code.google.com/p/goprotobuf/proto"
	"common"
	"protocol"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type ConcreteMsgFactory struct {
}

/////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func NewConcreteMsgFactory() *ConcreteMsgFactory {
	registerMessages()

	return &ConcreteMsgFactory{}
}

func (f *ConcreteMsgFactory) CreateProposal(txnid uint64,
	fid string,
	reqId uint64,
	op uint32,
	key string,
	content []byte) protocol.ProposalMsg {

	return &Proposal{Version: proto.Uint32(ProtoVersion()),
		Txnid:   proto.Uint64(txnid),
		Fid:     proto.String(fid),
		ReqId:   proto.Uint64(reqId),
		OpCode:  proto.Uint32(op),
		Key:     proto.String(key),
		Content: content}
}

func (f *ConcreteMsgFactory) CreateAccept(txnid uint64,
	fid string) protocol.AcceptMsg {

	return &Accept{Version: proto.Uint32(ProtoVersion()),
		Txnid: proto.Uint64(txnid),
		Fid:   proto.String(fid)}
}

func (f *ConcreteMsgFactory) CreateCommit(txnid uint64) protocol.CommitMsg {

	return &Commit{Version: proto.Uint32(ProtoVersion()),
		Txnid: proto.Uint64(txnid)}
}

func (f *ConcreteMsgFactory) CreateVote(round uint64,
	status uint32,
	epoch uint32,
	cndId string,
	loggedTxnId uint64,
	committedTxnId uint64) protocol.VoteMsg {

	return &Vote{Version: proto.Uint32(ProtoVersion()),
		Round:    proto.Uint64(round),
		Status:   proto.Uint32(status),
		Epoch:    proto.Uint32(epoch),
		CndId:    proto.String(cndId),
		CndLoggedTxnId: proto.Uint64(loggedTxnId),
		CndCommittedTxnId: proto.Uint64(committedTxnId)}
}

func (f *ConcreteMsgFactory) CreateLogEntry(txnid uint64,
	opCode uint32,
	key string,
	content []byte) protocol.LogEntryMsg {

	return &LogEntry{Version: proto.Uint32(ProtoVersion()),
		Txnid:   proto.Uint64(uint64(txnid)),
		OpCode:  proto.Uint32(opCode),
		Key:     proto.String(key),
		Content: content}
}

func (f *ConcreteMsgFactory) CreateFollowerInfo(epoch uint32,
											    fid string) protocol.FollowerInfoMsg {

	return &FollowerInfo{Version: proto.Uint32(ProtoVersion()),
		AcceptedEpoch: proto.Uint32(epoch),
		Fid:     proto.String(fid)}
}

func (f *ConcreteMsgFactory) CreateLeaderInfo(epoch uint32) protocol.LeaderInfoMsg {

	return &LeaderInfo{Version: proto.Uint32(ProtoVersion()),
		AcceptedEpoch: proto.Uint32(epoch)}
}

func (f *ConcreteMsgFactory) CreateEpochAck(txid uint64, epoch uint32) protocol.EpochAckMsg {

	return &EpochAck{Version: proto.Uint32(ProtoVersion()),
		LastLoggedTxid: proto.Uint64(txid),
		CurrentEpoch:   proto.Uint32(epoch)}
}

func (f *ConcreteMsgFactory) CreateNewLeader(epoch uint32) protocol.NewLeaderMsg {

	return &NewLeader{Version: proto.Uint32(ProtoVersion()),
		CurrentEpoch: proto.Uint32(epoch)}
}

func (f *ConcreteMsgFactory) CreateNewLeaderAck() protocol.NewLeaderAckMsg {

	return &NewLeaderAck{Version: proto.Uint32(ProtoVersion())}
}

func (f *ConcreteMsgFactory) CreateRequest(reqid uint64,
	opCode uint32,
	key string,
	content []byte) protocol.RequestMsg {

	return &Request{Version: proto.Uint32(ProtoVersion()),
		ReqId:   proto.Uint64(reqid),
		OpCode:  proto.Uint32(opCode),
		Key:     proto.String(key),
		Content: content}
}

/////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func registerMessages() {
	common.RegisterPacketByName("Proposal", &Proposal{})
	common.RegisterPacketByName("Accept", &Accept{})
	common.RegisterPacketByName("Commit", &Commit{})
	common.RegisterPacketByName("Vote", &Vote{})
	common.RegisterPacketByName("LogEntry", &LogEntry{})
	common.RegisterPacketByName("FollowerInfo", &FollowerInfo{})
	common.RegisterPacketByName("LeaderInfo", &LeaderInfo{})
	common.RegisterPacketByName("EpochAck", &EpochAck{})
	common.RegisterPacketByName("NewLeader", &NewLeader{})
	common.RegisterPacketByName("NewLeaderAck", &NewLeaderAck{})
	common.RegisterPacketByName("Request", &Request{})
}
