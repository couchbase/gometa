package repository

import (
	"common"
	"message"
	"strconv"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// CommitLog
/////////////////////////////////////////////////////////////////////////////

type CommitLog struct {
	repo            *Repository
	factory         *message.ConcreteMsgFactory
	mutex           sync.Mutex
}

type LogIterator struct {
	repo    *Repository
	iter    *RepoIterator
	hasLock bool
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
func (r *CommitLog) NewIterator(txid common.Txnid, exclusive bool) (*LogIterator, error) {

	startKey := createLogKey(txid)
	iter, err := r.repo.NewIterator(startKey, "")
	if err != nil {
		return nil, err
	}

	if exclusive {
		r.repo.Lock()
	}

	result := &LogIterator{iter: iter,
		repo:    r.repo,
		hasLock: exclusive}
	return result, nil
}

// Get value from iterator
func (i *LogIterator) Next() (txnid common.Txnid, op common.OpCode, key string, content []byte, err error) {

	// TODO: Check if fdb and iterator is closed
	key, content, err = i.iter.Next()
	if err != nil {
		return 0, common.OPCODE_INVALID, "", nil, err
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

	if i.hasLock {
		i.repo.Unlock()
	}

	i.iter.Close()
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func createLogKey(txid common.Txnid) string {

	buf := []byte(common.PREFIX_COMMIT_LOG_PATH)
	buf = strconv.AppendInt(buf, int64(txid), 10)

	return string(buf)
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
