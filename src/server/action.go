package server

import (
	"common"
	"protocol"
	repo "repository"
	"log"	
)

////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type ServerAction struct {
	repo    *repo.Repository
	log     *repo.CommitLog
	config  *repo.ServerConfig
	server  ServerCallback
	factory protocol.MsgFactory
}

////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func NewServerAction(s *Server) *ServerAction {

	return &ServerAction{repo: s.repo,
		log:     s.log,
		server:  s,
		config:  s.srvConfig,
		factory: s.factory}
}

////////////////////////////////////////////////////////////////////////////
// Server Action for Environment 
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetEnsembleSize() uint64 {
	return uint64(len(GetPeerUDPAddr())) + 1  // including myself 
}

////////////////////////////////////////////////////////////////////////////
// Server Action for Broadcast stage (normal execution)
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) Commit(p protocol.ProposalMsg) error {

	// TODO: Make the whole func transactional
	err := a.persistChange(common.OpCode(p.GetOpCode()), p)
	if err != nil {
		return err
	}
	
	a.config.SetLastCommittedTxid(p.GetTxnid())

	a.server.UpdateStateOnCommit(p)

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

func (a *ServerAction) GetNextTxnId() common.Txnid {
	return common.GetNextTxnId()
}

func (a *ServerAction) GetFollowerId() string {
	return GetHostTCPAddr() 
}

////////////////////////////////////////////////////////////////////////////
// Server Action for retrieving repository state
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetLastLoggedTxid() (common.Txnid, error) {
	val, err := a.config.GetLastLoggedTxnId()
	return common.Txnid(val), err
}

func (a *ServerAction) GetBootstrapLastLoggedTxid() common.Txnid {
	return common.Txnid(a.config.GetBootstrapLastLoggedTxnId())
}

func (a *ServerAction) GetLastCommittedTxid() (common.Txnid, error) {
	val, err := a.config.GetLastCommittedTxnId()
	return common.Txnid(val), err
}

func (a *ServerAction) GetBootstrapLastCommittedTxid() common.Txnid {
	return common.Txnid(a.config.GetBootstrapLastCommittedTxnId())
}

func (a *ServerAction) GetStatus() protocol.PeerStatus {
	return a.server.GetState().getStatus()
}

func (a *ServerAction) GetCurrentEpoch() (uint32, error) {
	return a.config.GetCurrentEpoch()
}

func (a *ServerAction) GetBootstrapCurrentEpoch() uint32 {
	return a.config.GetBootstrapCurrentEpoch()
}

func (a *ServerAction) GetAcceptedEpoch() (uint32, error) {
	return a.config.GetAcceptedEpoch()
}

func (a *ServerAction) GetBootstrapAcceptedEpoch() uint32 {
	return a.config.GetBootstrapAcceptedEpoch()
}

////////////////////////////////////////////////////////////////////////////
// Server Action for updating repository state
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) NotifyNewAcceptedEpoch(epoch uint32) {
	oldEpoch, _ := a.GetAcceptedEpoch()
	
	// update only if the new epoch is larger
	if oldEpoch < epoch {  
		a.config.SetAcceptedEpoch(epoch)
	}
}

func (a *ServerAction) NotifyNewCurrentEpoch(epoch uint32) {
	oldEpoch, _ := a.GetCurrentEpoch()
	
	// update only if the new epoch is larger
	if oldEpoch < epoch {  
		a.config.SetCurrentEpoch(epoch)
		a.server.UpdateWinningEpoch(epoch)
	}
}

func (a *ServerAction) NotifyNewLastCommittedTxid(txid uint64) {

	a.config.SetLastCommittedTxid(txid)
}

////////////////////////////////////////////////////////////////////////////
// Function for discovery phase
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetCommitedEntries(txid uint64) (chan protocol.LogEntryMsg, chan error, error) {

	// Get an iterator thas has exclusive write access.  This means there will not be
	// new commit entry being written while iterating.
	iter, err := a.log.NewIterator(common.Txnid(txid), true)
	if err != nil {
		return nil, nil, err
	}

	logChan := make(chan protocol.LogEntryMsg)
	errChan := make(chan error)

	go a.startLogStreamer(txid, iter, logChan, errChan)

	return logChan, errChan, nil
}

func (a *ServerAction) startLogStreamer(startTxid uint64,
	iter *repo.LogIterator,
	logChan chan protocol.LogEntryMsg,
	errChan chan error) {

	// Close the iterator upon termination
	defer iter.Close()

	// TODO : Need to lock the commitLog so there is no new commit while streaming
	
	// stream the first entry with given txid
	msg := a.factory.CreateLogEntry(startTxid, uint32(common.OPCODE_STREAM_BEGIN_MARKER), "StreamBegin", ([]byte)("StreamBegin"))
	logChan <- msg
	
	txnid, op, key, body, err := iter.Next() 
	for err == nil  {
		// only stream entry with a txid greater than the given one.  The caller would already 
		// have the entry for startTxid. If the caller use the boostrap value for txnid (0),
		// then this will stream everything.
		if uint64(txnid) > startTxid {
			msg := a.factory.CreateLogEntry(uint64(txnid), uint32(op), key, body)
			logChan <- msg
		}
		txnid, op, key, body, err = iter.Next()
	}

	// stream the last entry with the committed txid 
	lastCommitted, err := a.GetLastCommittedTxid() 
	msg = a.factory.CreateLogEntry(uint64(lastCommitted), uint32(common.OPCODE_STREAM_END_MARKER), "StreamEnd", ([]byte)("StreamEnd"))
	logChan <- msg

	// Nothing more to send.  The entries will be in the channel until the reciever consumes them. 
	close(logChan)
	close(errChan)
}

func (a *ServerAction) AppendLog(txid uint64, op uint32, key string, content []byte) error {

	return a.appendCommitLog(common.Txnid(txid), common.OpCode(op), key, content)
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) persistChange(op common.OpCode, p protocol.ProposalMsg) error {

	if op == common.OPCODE_SET {
		return a.repo.Set(p.GetKey(), p.GetContent())
	}

	if op == common.OPCODE_DELETE {
		return a.repo.Delete(p.GetKey())
	}

	// TODO: Return error: invalid operation
	return nil
}

func (a *ServerAction) appendCommitLog(txnid common.Txnid, opCode common.OpCode, key string, content []byte) error {

	// TODO: Make the whole func transactional
	err := a.log.Log(txnid, opCode, key, content)
	if err != nil {
		return err
	}
	
	a.config.SetLastLoggedTxid(uint64(txnid))
	
	return nil
}
