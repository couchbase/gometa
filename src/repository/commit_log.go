package repository

import (
	"strconv"
	"common"
	"message"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// CommitLog
/////////////////////////////////////////////////////////////////////////////

type CommitLog struct {
	repo             *Repository
	factory          *message.ConcreteMsgFactory
	lastLoggedTxnid   common.Txnid
	mutex             sync.Mutex
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new commit log
//
func NewCommitLog(repo *Repository) *CommitLog {
	return &CommitLog{repo : repo, factory : message.NewConcreteMsgFactory()}
}

//
// Add Entry to commit log 
//
func (r *CommitLog) Log(txid common.Txnid, op common.OpCode, key string, content []byte) error {

    k := createKey(txid)
    msg := r.factory.CreateLogEntry(uint32(op), key, content)
   	data, err := common.Marshall(msg) 
   	if err != nil {
   		return err
   	}
    
	err = r.repo.Set(k, data)	
	if err != nil {
		return err
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()	
	r.lastLoggedTxnid = txid
	
	return nil
}

// 
// Get the last logged Txnid
//
func (r *CommitLog) GetLastLoggedTxnId() common.Txnid {
	r.mutex.Lock()
	defer r.mutex.Unlock()	
	return r.lastLoggedTxnid
}

//
// Retrieve entry from commit log
//
func (r *CommitLog) Get(txid common.Txnid) (common.OpCode, string, []byte, error) {

    k := createKey(txid) 
    data, err := r.repo.Get(k) 
    if err != nil {
    	return common.OPCODE_INVALID, "", nil, err
    }
    
   	packet, err := common.UnMarshall(data[8:])
   	if err != nil {
   		return common.OPCODE_INVALID, "", nil, err
   	}
   	entry := packet.(*message.LogEntry)
   	return common.GetOpCodeFromInt(entry.GetOpCode()), entry.GetKey(), entry.GetContent(), nil
}

//
// Delete from commit log 
//
func (r *CommitLog) Delete(txid common.Txnid) error {

    k := createKey(txid)
    return r.repo.Delete(k) 
}

////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func createKey(txid common.Txnid) (string) {
	
    buf := []byte(common.PREFIX_COMMIT_LOG_PATH)
    buf = strconv.AppendInt(buf, int64(txid), 10)

   	return string(buf)
}
