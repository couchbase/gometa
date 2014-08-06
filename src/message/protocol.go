package message

import (
	"code.google.com/p/goprotobuf/proto"
)

//
// Proposal - implement Packet interface
//
func (req *Proposal) Name() string {
	return "Proposal"
}

func (req *Proposal) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Proposal) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// Accept - implement Packet interface
//
func (req *Accept) Name() string {
	return "Accept"
}

func (req *Accept) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Accept) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// Commit - implement Packet interface
//
func (req *Commit) Name() string {
	return "Commit"
}

func (req *Commit) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Commit) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// Vote - implement Packet interface
//
func (req *Vote) Name() string {
	return "Vote"
}

func (req *Vote) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Vote) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// Ballot - implement Packet interface
//
func (req *Ballot) Name() string {
	return "Ballot"
}

func (req *Ballot) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Ballot) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// LogEntry - implement Packet interface
//
func (req *LogEntry) Name() string {
	return "LogEntry"
}

func (req *LogEntry) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *LogEntry) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// FollowerInfo - implement Packet interface
//
func (req *FollowerInfo) Name() string {
	return "FollowerInfo"
}

func (req *FollowerInfo) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *FollowerInfo) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// EpochAck - implement Packet interface
//
func (req *EpochAck) Name() string {
	return "EpochAck"
}

func (req *EpochAck) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *EpochAck) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// LeaderInfo - implement Packet interface
//
func (req *LeaderInfo) Name() string {
	return "LeaderInfo"
}

func (req *LeaderInfo) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *LeaderInfo) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// NewLeader - implement Packet interface
//
func (req *NewLeader) Name() string {
	return "NewLeader"
}

func (req *NewLeader) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *NewLeader) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// NewLeaderAck - implement Packet interface
//
func (req *NewLeaderAck) Name() string {
	return "NewLeaderAck"
}

func (req *NewLeaderAck) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *NewLeaderAck) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//
// Request - implement Packet interface
//
func (req *Request) Name() string {
	return "Request"
}

func (req *Request) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Request) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}
