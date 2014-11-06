// @author Couchbase <info@couchbase.com>
// @copyright 2014 NorthScale, Inc.
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
	"log"
	"strconv"
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

func (req *Proposal) Print() {
	log.Printf("Proposal Message:")
	log.Printf("	Txnid  : %d", req.GetTxnid())
	log.Printf("	Fid    : %s", req.GetFid())
	log.Printf("	ReqId  : %d", req.GetReqId())
	log.Printf("	OpCode : %d", req.GetOpCode())
	log.Printf("	Key    : %s", req.GetKey())
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

func (req *Accept) Print() {
	log.Printf("Accept Message:")
	log.Printf("	Txnid : %d", req.GetTxnid())
	log.Printf("	Fid   : %s", req.GetFid())
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

func (req *Commit) Print() {
	log.Printf("Commit Message:")
	log.Printf("	Txnid : %d", req.GetTxnid())
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

func (req *Vote) Print() {
	log.Printf("Vote Message:")
	log.Printf("	Round           : %d", req.GetRound())
	log.Printf("	Status          : %d", req.GetStatus())
	log.Printf("	Epoch           : %d", req.GetEpoch())
	log.Printf("	Candidate Id    : %s", req.GetCndId())
	log.Printf("	Logged TxnId    : %d", req.GetCndLoggedTxnId())
	log.Printf("	Committed TxnId : %d", req.GetCndCommittedTxnId())
	log.Printf("	SolicitOnly     : %s", strconv.FormatBool(req.GetSolicit()))
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

func (req *LogEntry) Print() {
	log.Printf("LogEntry Message:")
	log.Printf("	Txnid  : %d", req.GetTxnid())
	log.Printf("	Key    : %s", req.GetKey())
	log.Printf("	OpCode : %d", req.GetOpCode())
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

func (req *FollowerInfo) Print() {
	log.Printf("FollowerInfo Message:")
	log.Printf("	AcceptedEpoch : %d", req.GetAcceptedEpoch())
	log.Printf("	Voting        : %s", strconv.FormatBool(req.GetVoting()))
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

func (req *EpochAck) Print() {
	log.Printf("EpochAck Message:")
	log.Printf("	LastLoggedTxid : %d", req.GetLastLoggedTxid())
	log.Printf("	CurrentEpoch : %d", req.GetCurrentEpoch())
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

func (req *LeaderInfo) Print() {
	log.Printf("LeaderInfo Message:")
	log.Printf("	AcceptedEpoch : %d", req.GetAcceptedEpoch())
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

func (req *NewLeader) Print() {
	log.Printf("NewLeader Message:")
	log.Printf("	CurrentEpoch : %d", req.GetCurrentEpoch())
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

func (req *NewLeaderAck) Print() {
	log.Printf("NewLeaderAck Message: No field to print")
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

func (req *Request) Print() {
	log.Printf("Request Message:")
	log.Printf("	ReqId  : %d", req.GetReqId())
	log.Printf("	OpCode : %d", req.GetOpCode())
	log.Printf("	Key    : %s", req.GetKey())
}
