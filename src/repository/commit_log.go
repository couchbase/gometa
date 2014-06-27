package repository

import (
	"strconv"
	"common"
	"message"
)

/////////////////////////////////////////////////////////////////////////////
// CommitLog
/////////////////////////////////////////////////////////////////////////////

type CommitLog struct {
	repo    *Repository
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new commit log
//
func NewCommitLog(repo *Repository) *CommitLog {
	return &CommitLog{repo : repo}
}

//
// Add Entry to commit log 
//
func (r *CommitLog) Log(txid Txnid, op string, key string, content []byte) error {

    k := createKey(txid)
    msg := message.CreateLogEntry(op, key, content)
   	data, err := common.Marshall(msg) 
   	if err != nil {
   		return err
   	}
    
	return r.Set(k, data)	
}

//
// Retrieve entry from commit log
//
func (r *CommitLog) Get(txid Txnid) (string, string, []byte, error) {

    k := createKey(txid) 
    data, err := r.Get(k) 
    if err != nil {
    	return nil, nil, nil, err
    }
    
   	packet, err := common.UnMarshaller(data[8:])
   	if err != nil {
   		return err
   	}
   	entry := packet.(message.LogEntry)
   	return entry.GetOpCode(), entry.GetKey(), entry.GetContent()
}

//
// Delete from commit log 
//
func (r *CommitLog) Delete(txid Txnid) error {

    k := createKey(txid)
    return r.Delete(k) 
}

////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func createKey(txid Txnid) (string, error) {
	
    buf := []byte(common.PREFIX_COMMIT_LOG_PATH)
    buf = strconv.AppendInt(buf, int64(txid), 10)

   	return string(k), nil 
}
