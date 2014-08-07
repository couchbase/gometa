package server

import (
	"common"
	"protocol"
	repo "repository"
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
// Server Action for Broadcast stage (normal execution)
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) Commit(p protocol.ProposalMsg) error {

	err := a.persistChange(common.OpCode(p.GetOpCode()), p)
	if err != nil {
		return err
	}

	err = a.appendCommitLog(common.Txnid(p.GetTxnid()), common.OpCode(p.GetOpCode()), p.GetKey(), p.GetContent())
	if err != nil {
		return err
	}

	// TODO : Commit
	/*
	   err = a.repo.Commit()
	   if err != nil {
	   	return err
	   }
	*/

	a.server.UpdateStateOnCommit(p)

	return nil
}

func (a *ServerAction) LogProposal(p protocol.ProposalMsg) error {

	a.server.UpdateStateOnNewProposal(p)

	return nil
}

func (a *ServerAction) GetNextTxnId() common.Txnid {
	return common.GetNextTxnId()
}

////////////////////////////////////////////////////////////////////////////
// Server Action for retrieving repository state
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetLastLoggedTxid() common.Txnid {
	return a.log.GetLastLoggedTxnId()
}

func (a *ServerAction) GetStatus() protocol.PeerStatus {
	return a.server.GetState().getStatus()
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

func (a *ServerAction) NotifyNewAcceptedEpoch(epoch uint32) {
	a.config.SetAcceptedEpoch(epoch)
}

func (a *ServerAction) NotifyNewCurrentEpoch(epoch uint32) {
	a.config.SetCurrentEpoch(epoch)
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
	txnid, op, key, body, err := iter.Next() 
	for err == nil  {
		msg := a.factory.CreateLogEntry(uint64(txnid), uint32(op), key, body)
		logChan <- msg
		txnid, op, key, body, err = iter.Next()
	}

	// stream the last entry with txid again
	msg := a.factory.CreateLogEntry(startTxid, uint32(common.OPCODE_STREAM_END_MARKER), "StreamEnd", []byte("StreamEnd"))
	logChan <- msg

	// TODO : The item is supposed to be with even if the channel is closed. Double check.
	close(logChan)
	close(errChan)
}

func (a *ServerAction) CommitEntry(txid uint64, op uint32, key string, content []byte) error {

	return a.log.Log(common.Txnid(txid), common.OpCode(op), key, content)
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

	return a.log.Log(txnid, opCode, key, content)
}
