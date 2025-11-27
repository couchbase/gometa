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

package repository

import (
	"fmt"

	"github.com/couchbase/gometa/common"
	logging "github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/message"

	// fdb "github.com/couchbase/goforestdb"
	"sync"
	"time"

	fdb "github.com/couchbase/indexing/secondary/fdb"
)

/////////////////////////////////////////////////////////////////////////////
// TransientCommitLog
/////////////////////////////////////////////////////////////////////////////

type TransientCommitLog struct {
	repo    IRepository
	factory *message.ConcreteMsgFactory
	logs    map[common.Txnid]*message.LogEntry
	mutex   sync.Mutex
}

type TransientLogIterator struct {
	repo  IRepository
	txnid common.Txnid
	iter  IRepoIterator

	curTxnid   common.Txnid
	curKey     string
	curContent []byte
	curError   error
}

/////////////////////////////////////////////////////////////////////////////
// CommitLog Public Function
/////////////////////////////////////////////////////////////////////////////

// Create a new commit log
func NewTransientCommitLog(repo IRepository, lastCommittedTxnid common.Txnid) (CommitLogger, error) {

	log := &TransientCommitLog{
		repo:    repo,
		factory: message.NewConcreteMsgFactory(),
		logs:    make(map[common.Txnid]*message.LogEntry)}

	if lastCommittedTxnid != common.BOOTSTRAP_LAST_COMMITTED_TXID {
		if err := repo.CreateSnapshot(MAIN, lastCommittedTxnid); err != nil {
			logging.Current.Errorf("NewTransientCommitLog: Cannot create initial snapshot")
			return nil, err
		}
	}

	return log, nil
}

// Add Entry to commit log
func (r *TransientCommitLog) Log(txid common.Txnid, op common.OpCode, key string, content []byte) error {

	msg := r.factory.CreateLogEntry(uint64(txid), uint32(op), key, content)
	r.logs[txid] = msg.(*message.LogEntry)
	return nil
}

// Retrieve entry from commit log
func (r *TransientCommitLog) Get(txid common.Txnid) (common.OpCode, string, []byte, error) {

	msg, ok := r.logs[txid]
	if !ok || msg == nil {
		err := common.NewError(common.REPO_ERROR, fmt.Sprintf("LogEntry for txid %d does not exist in commit log", txid))
		return common.OPCODE_INVALID, "", nil, err
	}

	// msg is a protobuf object. Directly pointing slices/strings into a struct
	// may cause problems for cgo calls
	key := string([]byte(msg.GetKey()))
	content := []byte(string(msg.GetContent()))
	return common.GetOpCodeFromInt(msg.GetOpCode()), key, content, nil
}

// Delete from commit log
func (r *TransientCommitLog) Delete(txid common.Txnid) error {
	delete(r.logs, txid)
	return nil
}

// Mark a log entry has been committted.   This function is called when there
// is no concurrent commit for the given txid to match the repo snapshot.
func (r *TransientCommitLog) MarkCommitted(txid common.Txnid) error {

	delete(r.logs, txid)
	if err := r.repo.CreateSnapshot(MAIN, txid); err != nil {
		return err
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// LogIterator Public Function
/////////////////////////////////////////////////////////////////////////////

// Create a new iterator.   This is used by LeaderSyncProxy to replicate log
// entries to a folower/watcher.  txid1 is last logged commited txid from follower/watcher.
// txid2 is the txid of the first packet in the observer queue for this follower/watcher.
// If the observer queue is empty, then txid2 is 0.
//
// For any LogEntry returned form the iterator, the following condition must be satisifed:
//  1. The txid of any LogEntry returned from the iterator must be greater than or equal to txid1.
//  2. The last LogEntry must have txid >= txid2.  In other words, the iterator can gaurantee
//     that it will cover any mutation by txid2, but cannot guarantee that it stops at txid2.
//
// Since TransientCommitLog does not keep track of history, it only supports the following case:
// 1) the beginning txid (txid1) is 0 (sending the full repository)
func (r *TransientCommitLog) NewIterator(txid1, txid2 common.Txnid) (CommitLogIterator, error) {

	if txid1 != 0 {
		return nil, common.NewError(common.REPO_ERROR, "TransientLCommitLog.NewIterator: cannot support beginning txid > 0")
	}

	// iter can be nil if nothing has been logged
	retry := true

retryLabel:
	txnid, iter, err := r.repo.AcquireSnapshot(MAIN)
	if err != nil {
		return nil, err
	}

	// The snapshot txnid must be at least match the ending txid (txid2).  If not, it will retry.
	// This is to ensure that it takes care race condition in which the first msg in an observer's
	// queue is a commit, and the caller asks for the state of the repository which is at least as
	// recent as that commit msg (txid2).  If retry fails, return an error.
	if txnid < txid2 {
		if retry {
			time.Sleep(common.XACT_COMMIT_WAIT_TIME)
			retry = false
			goto retryLabel
		}

		return nil, common.NewError(common.REPO_ERROR,
			fmt.Sprintf("TransientLCommitLog.NewIterator: cannot support ending txid > %d", txnid))
	}

	result := &TransientLogIterator{
		iter:       iter,
		txnid:      txnid,
		repo:       r.repo,
		curTxnid:   common.Txnid(txid1 + 1),
		curKey:     "",
		curContent: nil,
		curError:   nil}

	if result.iter != nil {
		result.curKey, result.curContent, result.curError = result.iter.Next()
	}

	return result, nil
}

// Get value from iterator
func (i *TransientLogIterator) Next() (txnid common.Txnid, op common.OpCode, key string, content []byte, err error) {

	if i.iter == nil {
		return 0, common.OPCODE_INVALID, "", nil, fdb.RESULT_ITERATOR_FAIL
	}

	if i.curError != nil {
		return 0, common.OPCODE_INVALID, "", nil, i.curError
	}

	key = i.curKey
	content = i.curContent
	txnid = i.curTxnid

	i.curTxnid = common.Txnid(uint64(i.curTxnid) + 1)
	i.curKey, i.curContent, i.curError = i.iter.Next()

	if i.curError == nil {
		// it is not the last entry. Does not matter what is the actual txnid as long as it is
		// smaller than the snapshot's txnid (TransientLogIterator.txnid).
		return txnid, common.OPCODE_SET, key, content, nil
	}

	// last entry : use the txnid matching the snapshot
	return i.txnid, common.OPCODE_SET, key, content, nil
}

// close iterator
func (i *TransientLogIterator) Close() {

	// TODO: Check if fdb iterator is closed
	if i.iter != nil {
		i.iter.Close()
	}
	i.repo.ReleaseSnapshot(MAIN, i.txnid)
}
