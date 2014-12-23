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
	"github.com/couchbase/gometa/message"
	"strings"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// TransientCommitLog
/////////////////////////////////////////////////////////////////////////////

type TransientCommitLog struct {
	repo    *Repository
	factory *message.ConcreteMsgFactory
	logs    map[common.Txnid]*message.LogEntry
	mutex   sync.Mutex
}

type TransientLogIterator struct {
	repo  *Repository
	txnid common.Txnid
	iter  *RepoIterator

	curTxnid   common.Txnid
	curKey     string
	curContent []byte
	curError   error
}

/////////////////////////////////////////////////////////////////////////////
// CommitLog Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new commit log
//
func NewTransientCommitLog(repo *Repository) CommitLogger {
	return &TransientCommitLog{
		repo:    repo,
		factory: message.NewConcreteMsgFactory(),
		logs:    make(map[common.Txnid]*message.LogEntry)}
}

//
// Add Entry to commit log
//
func (r *TransientCommitLog) Log(txid common.Txnid, op common.OpCode, key string, content []byte) error {

	msg := r.factory.CreateLogEntry(uint64(txid), uint32(op), key, content)
	r.logs[txid] = msg.(*message.LogEntry)
	return nil
}

//
// Retrieve entry from commit log
//
func (r *TransientCommitLog) Get(txid common.Txnid) (common.OpCode, string, []byte, error) {

	msg, ok := r.logs[txid]
	if !ok || msg == nil {
		err := common.NewError(common.REPO_ERROR, fmt.Sprintf("LogEntry for txid %d does not exist in commit log", txid))
		return common.OPCODE_INVALID, "", nil, err
	}

	return common.GetOpCodeFromInt(msg.GetOpCode()), msg.GetKey(), msg.GetContent(), nil
}

//
// Delete from commit log
//
func (r *TransientCommitLog) Delete(txid common.Txnid) error {
	delete(r.logs, txid)
	return nil
}

//
// Mark a log entry has been committted.   This function is called when there
// is no concurrent commit for the given txid to match the repo snapshot.
//
func (r *TransientCommitLog) MarkCommitted(txid common.Txnid) error {

	delete(r.logs, txid)
	if err := r.repo.CreateSnapshot(txid); err != nil {
		return err
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// LogIterator Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator.   This is used by LeaderSyncProxy to replicate log
// entries to a folower/watcher.  txid1 is last logged commited txid from follower/watcher.
// txid2 is the txid of the first packet in the observer queue for this follower/watcher.
// If the observer queue is empty, then txid2 is 0.
//
// For any LogEntry returned form the iterator, the following condition must be satisifed:
// 1) The txid of any LogEntry returned from the iterator must be greater than or equal to txid1.
// 2) The last LogEntry must have txid >= txid2.  In other words, the iterator can gaurantee
//    that it will cover any mutation by txid2, but cannot guarantee that it stops at txid2.
//
// Since TransientCommitLog does not keep track of history, it only supports the following case:
// 1) the beginning txid (txid1) is 0 (sending the full repository)
//
func (r *TransientCommitLog) NewIterator(txid1, txid2 common.Txnid) (CommitLogIterator, error) {

	if txid1 != 0 {
		return nil, common.NewError(common.REPO_ERROR, "TransientLCommitLog.NewIterator: cannot support beginning txid > 0")
	}

	txnid, iter, err := r.repo.AcquireSnapshot()
	if err != nil {
		return nil, err
	}

	if txnid < txid2 {
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

	for {
		result.curKey, result.curContent, result.curError = result.iter.Next()
		if result.curError != nil || strings.Contains(result.curKey, common.PREFIX_DATA_PATH) {
			break
		}
	}

	return result, nil
}

// Get value from iterator
func (i *TransientLogIterator) Next() (txnid common.Txnid, op common.OpCode, key string, content []byte, err error) {

	if i.curError != nil {
		return 0, common.OPCODE_INVALID, "", nil, i.curError
	}

	key = i.curKey
	content = i.curContent
	txnid = i.curTxnid

	// TODO: Check if fdb and iterator is closed
	i.curTxnid = common.Txnid(uint64(i.curTxnid) + 1)

	for {
		i.curKey, i.curContent, i.curError = i.iter.Next()
		if i.curError != nil || strings.Contains(i.curKey, common.PREFIX_DATA_PATH) {
			break
		}
	}

	idx := strings.Index(key, common.PREFIX_DATA_PATH)
	if idx != -1 {
		key = key[idx+len(common.PREFIX_DATA_PATH):]
	}

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
	i.iter.Close()
	i.repo.ReleaseSnapshot(i.txnid)
}
