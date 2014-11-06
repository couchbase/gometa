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

package action

import (
	"fmt"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	repo "github.com/couchbase/gometa/repository"
)

////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type ServerCallback interface {
	GetStatus() protocol.PeerStatus
	UpdateStateOnNewProposal(proposal protocol.ProposalMsg)
	UpdateStateOnCommit(txnid common.Txnid, key string)
	UpdateWinningEpoch(epoch uint32)
	GetEnsembleSize() uint64
	GetFollowerId() string
}

type DefaultServerCallback interface {
	protocol.QuorumVerifier
	ServerCallback
}

type ServerAction struct {
	repo     *repo.Repository
	log      *repo.CommitLog
	config   *repo.ServerConfig
	txn      *common.TxnState
	server   ServerCallback
	factory  protocol.MsgFactory
	verifier protocol.QuorumVerifier
}

////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func NewDefaultServerAction(repository *repo.Repository,
	server DefaultServerCallback,
	txn *common.TxnState) *ServerAction {

	log := repo.NewCommitLog(repository)
	config := repo.NewServerConfig(repository)
	factory := message.NewConcreteMsgFactory()

	return &ServerAction{
		repo:     repository,
		log:      log,
		config:   config,
		txn:      txn,
		server:   server,
		factory:  factory,
		verifier: server}
}

func NewServerAction(repo *repo.Repository,
	log *repo.CommitLog,
	config *repo.ServerConfig,
	server ServerCallback,
	txn *common.TxnState,
	factory protocol.MsgFactory,
	verifier protocol.QuorumVerifier) *ServerAction {

	return &ServerAction{
		repo:     repo,
		log:      log,
		config:   config,
		txn:      txn,
		server:   server,
		factory:  factory,
		verifier: verifier}
}

////////////////////////////////////////////////////////////////////////////
// Server Action for Environment
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetEnsembleSize() uint64 {
	return a.server.GetEnsembleSize()
}

func (a *ServerAction) GetQuorumVerifier() protocol.QuorumVerifier {
	return a.verifier
}

////////////////////////////////////////////////////////////////////////////
// Server Action for Broadcast stage (normal execution)
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) Commit(txid common.Txnid) error {

	// TODO: Make the whole func transactional
	opCode, key, content, err := a.log.Get(txid)
	if err != nil {
		return err
	}

	if err := a.persistChange(opCode, key, content); err != nil {
		return err
	}

	if err := a.config.SetLastCommittedTxid(txid); err != nil {
		return err
	}

	a.server.UpdateStateOnCommit(txid, key)

	return nil
}

func (a *ServerAction) LogProposal(p protocol.ProposalMsg) error {

	err := a.appendCommitLog(common.Txnid(p.GetTxnid()), common.OpCode(p.GetOpCode()), p.GetKey(), p.GetContent())
	if err != nil {
		return err
	}

	a.server.UpdateStateOnNewProposal(p)

	return nil
}

func (a *ServerAction) GetFollowerId() string {
	return a.server.GetFollowerId()
}

func (a *ServerAction) GetNextTxnId() common.Txnid {
	return a.txn.GetNextTxnId()
}

////////////////////////////////////////////////////////////////////////////
// Server Action for retrieving repository state
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetLastLoggedTxid() (common.Txnid, error) {
	val, err := a.config.GetLastLoggedTxnId()
	return common.Txnid(val), err
}

func (a *ServerAction) GetLastCommittedTxid() (common.Txnid, error) {
	val, err := a.config.GetLastCommittedTxnId()
	return common.Txnid(val), err
}

func (a *ServerAction) GetStatus() protocol.PeerStatus {
	return a.server.GetStatus()
}

func (a *ServerAction) GetCurrentEpoch() (uint32, error) {
	return a.config.GetCurrentEpoch()
}

func (a *ServerAction) GetAcceptedEpoch() (uint32, error) {
	return a.config.GetAcceptedEpoch()
}

////////////////////////////////////////////////////////////////////////////
// Server Action for updating repository state
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) NotifyNewAcceptedEpoch(epoch uint32) error {
	oldEpoch, _ := a.GetAcceptedEpoch()

	// update only if the new epoch is larger
	if oldEpoch < epoch {
		err := a.config.SetAcceptedEpoch(epoch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *ServerAction) NotifyNewCurrentEpoch(epoch uint32) error {
	oldEpoch, _ := a.GetCurrentEpoch()

	// update only if the new epoch is larger
	if oldEpoch < epoch {
		err := a.config.SetCurrentEpoch(epoch)
		if err != nil {
			return err
		}
		a.server.UpdateWinningEpoch(epoch)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////
// Function for discovery phase
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetCommitedEntries(txid1, txid2 common.Txnid) (<-chan protocol.LogEntryMsg, <-chan error, chan<- bool, error) {

	// Get an iterator thas has exclusive write access.  This means there will not be
	// new commit entry being written while iterating.
	iter, err := a.log.NewIterator(txid1, txid2)
	if err != nil {
		return nil, nil, nil, err
	}

	logChan := make(chan protocol.LogEntryMsg, 100)
	errChan := make(chan error, 10)
	killChan := make(chan bool, 1)

	go a.startLogStreamer(txid1, iter, logChan, errChan, killChan)

	return logChan, errChan, killChan, nil
}

func (a *ServerAction) startLogStreamer(startTxid common.Txnid,
	iter *repo.LogIterator,
	logChan chan protocol.LogEntryMsg,
	errChan chan error,
	killChan chan bool) {

	// Close the iterator upon termination
	defer iter.Close()

	// TODO : Need to lock the commitLog so there is no new commit while streaming

	txnid, op, key, body, err := iter.Next()
	for err == nil {
		// only stream entry with a txid greater than the given one.  The caller would already
		// have the entry for startTxid. If the caller use the boostrap value for txnid (0),
		// then this will stream everything.
		if txnid > startTxid {
			msg := a.factory.CreateLogEntry(uint64(txnid), uint32(op), key, body)
			select {
			case logChan <- msg:
			case _ = <-killChan:
				break
			}
		}
		txnid, op, key, body, err = iter.Next()
	}

	// Nothing more to send.  The entries will be in the channel until the reciever consumes them.
	close(logChan)
	close(errChan)
}

func (a *ServerAction) LogAndCommit(txid common.Txnid, op uint32, key string, content []byte, toCommit bool) error {

	// TODO: Make this transactional

	if err := a.appendCommitLog(txid, common.OpCode(op), key, content); err != nil {
		return err
	}

	if toCommit {
		if err := a.persistChange(common.OpCode(op), key, content); err != nil {
			return err
		}

		return a.config.SetLastCommittedTxid(txid)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) Get(key string) ([]byte, error) {

	newKey := fmt.Sprintf("%s%s", common.PREFIX_DATA_PATH, key)
	return a.repo.Get(newKey)
}

func (a *ServerAction) Set(key string, content []byte) error {
	newKey := fmt.Sprintf("%s%s", common.PREFIX_DATA_PATH, key)
	return a.repo.Set(newKey, content)
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) persistChange(op common.OpCode, key string, content []byte) error {

	newKey := fmt.Sprintf("%s%s", common.PREFIX_DATA_PATH, key)

	if op == common.OPCODE_ADD {
		return a.repo.Set(newKey, content)
	}

	if op == common.OPCODE_SET {
		return a.repo.Set(newKey, content)
	}

	if op == common.OPCODE_DELETE {
		return a.repo.Delete(newKey)
	}

	return common.NewError(common.PROTOCOL_ERROR, fmt.Sprintf("ServerAction.persistChange() : Unknown op code %d", op))
}

func (a *ServerAction) appendCommitLog(txnid common.Txnid, opCode common.OpCode, key string, content []byte) error {

	// TODO: Make the whole func transactional
	if err := a.log.Log(txnid, opCode, key, content); err != nil {
		return err
	}

	if err := a.config.SetLastLoggedTxid(txnid); err != nil {
		return err
	}

	return nil
}
