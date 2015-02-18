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

package message

import (
	"code.google.com/p/goprotobuf/proto"
	"strconv"
	"bytes"
	"fmt"
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

func (req *Proposal) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Proposal Message:"))
	buf.WriteString(fmt.Sprintf("	Txnid  : %d", req.GetTxnid()))
	buf.WriteString(fmt.Sprintf("	Fid    : %s", req.GetFid()))
	buf.WriteString(fmt.Sprintf("	ReqId  : %d", req.GetReqId()))
	buf.WriteString(fmt.Sprintf("	OpCode : %d", req.GetOpCode()))
	buf.WriteString(fmt.Sprintf("	Key    : %s", req.GetKey()))
	return buf.String()
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

func (req *Accept) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Accept Message:"))
	buf.WriteString(fmt.Sprintf("	Txnid : %d", req.GetTxnid()))
	buf.WriteString(fmt.Sprintf("	Fid   : %s", req.GetFid()))
	return buf.String()
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

func (req *Commit) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Commit Message:"))
	buf.WriteString(fmt.Sprintf("	Txnid : %d", req.GetTxnid()))
	return buf.String()
}

//
// Abort - implement Packet interface
//
func (req *Abort) Name() string {
	return "Abort"
}

func (req *Abort) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Abort) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

func (req *Abort) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Abort Message:"))
	buf.WriteString(fmt.Sprintf("	Fid    : %s", req.GetFid()))
	buf.WriteString(fmt.Sprintf("	ReqId  : %d", req.GetReqId()))
	buf.WriteString(fmt.Sprintf("	Error : %s", req.GetError()))
	return buf.String()
}

//
// Response - implement Packet interface
//
func (req *Response) Name() string {
	return "Response"
}

func (req *Response) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Response) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

func (req *Response) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Response Message:"))
	buf.WriteString(fmt.Sprintf("	Fid         : %s", req.GetFid()))
	buf.WriteString(fmt.Sprintf("	ReqId       : %d", req.GetReqId()))
	buf.WriteString(fmt.Sprintf("	Error       : %s", req.GetError()))
	buf.WriteString(fmt.Sprintf("	len(Content): %d", len(req.GetContent())))
	return buf.String()
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

func (req *Vote) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Vote Message:"))
	buf.WriteString(fmt.Sprintf("	Round           : %d", req.GetRound()))
	buf.WriteString(fmt.Sprintf("	Status          : %d", req.GetStatus()))
	buf.WriteString(fmt.Sprintf("	Epoch           : %d", req.GetEpoch()))
	buf.WriteString(fmt.Sprintf("	Candidate Id    : %s", req.GetCndId()))
	buf.WriteString(fmt.Sprintf("	Logged TxnId    : %d", req.GetCndLoggedTxnId()))
	buf.WriteString(fmt.Sprintf("	Committed TxnId : %d", req.GetCndCommittedTxnId()))
	buf.WriteString(fmt.Sprintf("	SolicitOnly     : %s", strconv.FormatBool(req.GetSolicit())))
	return buf.String()
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

func (req *LogEntry) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("LogEntry Message:"))
	buf.WriteString(fmt.Sprintf("	Txnid  : %d", req.GetTxnid()))
	buf.WriteString(fmt.Sprintf("	Key    : %s", req.GetKey()))
	buf.WriteString(fmt.Sprintf("	OpCode : %d", req.GetOpCode()))
	return buf.String()
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

func (req *FollowerInfo) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("FollowerInfo Message:"))
	buf.WriteString(fmt.Sprintf("	AcceptedEpoch : %d", req.GetAcceptedEpoch()))
	buf.WriteString(fmt.Sprintf("	Voting        : %s", strconv.FormatBool(req.GetVoting())))
	return buf.String()
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

func (req *EpochAck) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("EpochAck Message:"))
	buf.WriteString(fmt.Sprintf("	LastLoggedTxid : %d", req.GetLastLoggedTxid()))
	buf.WriteString(fmt.Sprintf("	CurrentEpoch : %d", req.GetCurrentEpoch()))
	return buf.String()
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

func (req *LeaderInfo) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("LeaderInfo Message:"))
	buf.WriteString(fmt.Sprintf("	AcceptedEpoch : %d", req.GetAcceptedEpoch()))
	return buf.String()
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

func (req *NewLeader) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("NewLeader Message:"))
	buf.WriteString(fmt.Sprintf("	CurrentEpoch : %d", req.GetCurrentEpoch()))
	return buf.String()
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

func (req *NewLeaderAck) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("NewLeaderAck Message: No field to print"))
	return buf.String()
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

func (req *Request) DebugString() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Request Message:"))
	buf.WriteString(fmt.Sprintf("	ReqId  : %d", req.GetReqId()))
	buf.WriteString(fmt.Sprintf("	OpCode : %d", req.GetOpCode()))
	buf.WriteString(fmt.Sprintf("	Key    : %s", req.GetKey()))
	return buf.String()
}
