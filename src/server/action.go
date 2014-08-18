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
	
	a.config.SetLastCommittedTxid(common.Txnid(p.GetTxnid()))

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

func (a *ServerAction) GetLastCommittedTxid() (common.Txnid, error) {
	val, err := a.config.GetLastCommittedTxnId()
	return common.Txnid(val), err
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

func (a *ServerAction) NotifyNewLastCommittedTxid(txid common.Txnid) {

	a.config.SetLastCommittedTxid(txid)
}

////////////////////////////////////////////////////////////////////////////
// Function for discovery phase
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetCommitedEntries(txid1, txid2 common.Txnid) (<- chan protocol.LogEntryMsg, <- chan error, chan <- bool, error) {

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
	for err == nil  {
		// only stream entry with a txid greater than the given one.  The caller would already 
		// have the entry for startTxid. If the caller use the boostrap value for txnid (0),
		// then this will stream everything.
		if txnid > startTxid {
			msg := a.factory.CreateLogEntry(uint64(txnid), uint32(op), key, body)
			select {
				case logChan <- msg:
				case _ = <- killChan :
					break
			}
		}
		txnid, op, key, body, err = iter.Next()
	}

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
	
	a.config.SetLastLoggedTxid(txnid)
	
	return nil
}
