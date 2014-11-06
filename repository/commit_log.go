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
	"log"
	"strings"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// CommitLog
/////////////////////////////////////////////////////////////////////////////

type CommitLog struct {
	repo    *Repository
	factory *message.ConcreteMsgFactory
	mutex   sync.Mutex
}

type LogIterator struct {
	repo *Repository
	iter *RepoIterator
}

/////////////////////////////////////////////////////////////////////////////
// CommitLog Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new commit log
//
func NewCommitLog(repo *Repository) *CommitLog {
	return &CommitLog{repo: repo,
		factory: message.NewConcreteMsgFactory()}
}

//
// Add Entry to commit log
//
func (r *CommitLog) Log(txid common.Txnid, op common.OpCode, key string, content []byte) error {

	k := createLogKey(txid)
	msg := r.factory.CreateLogEntry(uint64(txid), uint32(op), key, content)
	data, err := common.Marshall(msg)
	if err != nil {
		return err
	}

	err = r.repo.Set(k, data)
	if err != nil {
		return err
	}

	return nil
}

//
// Retrieve entry from commit log
//
func (r *CommitLog) Get(txid common.Txnid) (common.OpCode, string, []byte, error) {

	k := createLogKey(txid)
	data, err := r.repo.Get(k)
	if err != nil {
		return common.OPCODE_INVALID, "", nil, err
	}

	entry, err := unmarshall(data)
	if err != nil {
		return common.OPCODE_INVALID, "", nil, err
	}
	return common.GetOpCodeFromInt(entry.GetOpCode()), entry.GetKey(), entry.GetContent(), nil
}

//
// Delete from commit log
//
func (r *CommitLog) Delete(txid common.Txnid) error {

	k := createLogKey(txid)
	return r.repo.Delete(k)
}

/////////////////////////////////////////////////////////////////////////////
// LogIterator Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator
//
func (r *CommitLog) NewIterator(txid1, txid2 common.Txnid) (*LogIterator, error) {

	startKey := createLogKey(txid1)
	endKey := ""    // get everything until the commit log is exhausted
	if txid2 != 0 { // if txid2 is not the bootstrap value
		endKey = createLogKey(txid2)
	}

	iter, err := r.repo.NewIterator(startKey, endKey)
	if err != nil {
		return nil, err
	}

	result := &LogIterator{
		iter: iter,
		repo: r.repo}

	return result, nil
}

// Get value from iterator
func (i *LogIterator) Next() (txnid common.Txnid, op common.OpCode, key string, content []byte, err error) {

	// TODO: Check if fdb and iterator is closed
	key, content, err = i.iter.Next()
	if err != nil {
		return 0, common.OPCODE_INVALID, "", nil, err
	}

	// Since actual data is stored in the same repository, make sure
	// we don't read them.
	log.Printf("CommitLog.Next() : Iterator read key %s", key)
	if !strings.HasPrefix(key, common.PREFIX_COMMIT_LOG_PATH) {
		return 0, common.OPCODE_INVALID, "", nil, common.NewError(common.REPO_ERROR, "Iteration for commit log done")
	}

	entry, err := unmarshall(content)
	if err != nil {
		return 0, common.OPCODE_INVALID, "", nil, err
	}

	return common.Txnid(entry.GetTxnid()),
		common.GetOpCodeFromInt(entry.GetOpCode()),
		entry.GetKey(), entry.GetContent(), nil
}

// close iterator
func (i *LogIterator) Close() {

	// TODO: Check if fdb iterator is closed
	i.iter.Close()
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func createLogKey(txid common.Txnid) string {

	return fmt.Sprintf("%s%d", common.PREFIX_COMMIT_LOG_PATH, int64(txid))
}

func unmarshall(data []byte) (*message.LogEntry, error) {

	// skip the first 8 bytes (total len)
	packet, err := common.UnMarshall(data[8:])
	if err != nil {
		return nil, err
	}

	entry := packet.(*message.LogEntry)
	return entry, nil
}
