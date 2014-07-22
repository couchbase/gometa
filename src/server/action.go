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
    server   ServerCallback
}

////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func NewServerAction(server ServerCallback, 
                     repo *repo.Repository, 
                     log *repo.CommitLog) *ServerAction {

    return &ServerAction{repo : repo,
                         log : log,
                         server : server}
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

	// TODO

    a.server.UpdateStateOnNewProposal(p)
    
   	return nil 
}

func (a *ServerAction) GetNextTxnId() common.Txnid {
	return common.Txnid(0) 
}

////////////////////////////////////////////////////////////////////////////
// Server Action for retrieving repository state 
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) GetLastLoggedTxid() common.Txnid {
	return common.Txnid(0) 
}

func (a *ServerAction) GetStatus() protocol.PeerStatus {
	return protocol.LEADING
}

func (a *ServerAction) GetCurrentEpoch() uint32 {
	return 0
}
	
func (a *ServerAction) GetAcceptedEpoch() uint32 {
	return 0
}

////////////////////////////////////////////////////////////////////////////
// Server Action for updating repository state 
/////////////////////////////////////////////////////////////////////////////

func (a *ServerAction) NotifyNewAcceptedEpoch(uint32) {
}

func (a *ServerAction) NotifyNewCurrentEpoch(uint32) {
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