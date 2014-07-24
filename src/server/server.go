package server

import (
	"protocol"
	"common"
	"sync"
	"message"
	r "repository"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type Server struct {
	provider    StateProvider 
	factory     protocol.MsgFactory
	handler     protocol.ActionHandler
}

type ServerState struct {
	status              protocol.PeerStatus 
	incomings           chan *RequestHandle
	pendings			map[uint64]*RequestHandle    // request id
	proposals           map[uint64]*RequestHandle    // txnid
	mutex               sync.Mutex
}

type RequestHandle struct {
	request 			 protocol.RequestMsg
	mutex                sync.Mutex
	condVar			    *sync.Cond
}

type StateProvider interface {
    GetState()	*ServerState
}

type ServerCallback interface {
	StateProvider
	UpdateStateOnNewProposal(proposal protocol.ProposalMsg) 
	UpdateStateOnCommit(proposal protocol.ProposalMsg) 
}

var gServer *Server = nil

/////////////////////////////////////////////////////////////////////////////
// Bootstrap 
/////////////////////////////////////////////////////////////////////////////

func (s *Server) bootstrap() error {

	host := GetHostName()
	peers := GetPeers()
	
	repo, err := r.OpenRepository()
	if err != nil {
		return err
	}

    log := r.NewCommitLog(repo)	
    config := r.NewServerConfig(repo)	
    
	s.factory = message.NewConcreteMsgFactory()
	s.handler = NewServerAction(s, repo, log, config, s.factory)

	// Create an election site to start leader election.	
	resultCh := make(chan string)
	site, err := protocol.CreateElectionSite(host, peers, s.factory, s.handler) 
	site.StartElection(resultCh)
	leader := <- resultCh   // blocked until leader is elected

	// If this host is the leader, then start the leader server.
	// Otherwise, start the followerServer.
	if leader == host {
	    s.provider, err = newLeaderServer(host, s.handler, s.factory) 
	} else {
		s.provider, err = newFollowerServer(host, leader, s.handler, s.factory)	
	}	
	if err != nil {
		return err
	}
	
	return nil
}

func main() {
	gServer = &Server{provider : nil,
	                  factory : nil,
	                  handler : nil}
	gServer.bootstrap()
}

/////////////////////////////////////////////////////////////////////////////
// ServerState
/////////////////////////////////////////////////////////////////////////////

//
// Create a new ServerState
//
func newServerState() *ServerState {

	incomings := make(chan *RequestHandle, common.MAX_PROPOSALS)
	pendings := make(map[uint64]*RequestHandle)
	proposals := make(map[uint64]*RequestHandle)
	state := &ServerState{incomings : incomings,
	                     pendings : pendings,
	                     proposals : proposals,
	                     status : protocol.ELECTING}
	                       
	return state                       
}

func (s *ServerState) getStatus() protocol.PeerStatus {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	return s.status
}

func (s *ServerState) setStatus(status protocol.PeerStatus) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	s.status = status
}

/////////////////////////////////////////////////////////////////////////////
// Client API 
/////////////////////////////////////////////////////////////////////////////

//
// Handle a new incoming request
//
func (s *Server) HandleNewRequest(req protocol.RequestMsg) {

	// TODO : Assign an unique id to the request msg			
	id := uint64(1)
	request := s.factory.CreateRequest(id, 
	                                 req.GetOpCode(), 
	                                 req.GetKey(), 
	                                 req.GetContent())
		                                 
	handle := newRequestHandle(request)

	handle.condVar.L.Lock()
	defer handle.condVar.L.Unlock()
	
	// push the request to a channel 	
	s.provider.GetState().incomings <- handle 

	// This goroutine will wait until the request has been processed.	
	handle.condVar.Wait()
}

//
// Create a new request handle
//
func newRequestHandle(req protocol.RequestMsg) *RequestHandle {
	handle := &RequestHandle{request : req}
	handle.condVar = sync.NewCond(&handle.mutex)
	return handle
}

/////////////////////////////////////////////////////////////////////////////
// ServerCallback Interface 
/////////////////////////////////////////////////////////////////////////////

//
// Callback when a new proposal arrives
//
func (s* Server) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {

	fid := proposal.GetFid()
	reqId := proposal.GetReqId()
	txnid := proposal.GetTxnid()

	// If this host is the one that sends the request to the leader	
	if fid == GetHostName() {
		s.provider.GetState().mutex.Lock()
		defer s.provider.GetState().mutex.Unlock()
	
		// look up the request handle from the pending list and 
		// move it to the proposed list	
		handle, ok := s.provider.GetState().pendings[reqId]
		if ok {
			delete(s.provider.GetState().pendings, reqId)				
			s.provider.GetState().proposals[txnid] = handle 
		} 
	} 
}

//
// Callback when a commit arrives 
//
func (s* Server) UpdateStateOnCommit(proposal protocol.ProposalMsg) {

	txnid := proposal.GetTxnid()
	
	s.provider.GetState().mutex.Lock()
	defer s.provider.GetState().mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and 
	// notify the waiting goroutine that the request is done.	
	handle, ok := s.provider.GetState().proposals[txnid]
	if ok {
		delete(s.provider.GetState().proposals, txnid)
		
		handle.condVar.L.Lock()
		defer handle.condVar.L.Unlock()
		
		handle.condVar.Signal()
	}
}

func (s* Server)  GetState() *ServerState {
	return s.provider.GetState()
}