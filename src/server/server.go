package server

import (
	"protocol"
	"common"
	"sync"
	"message"
	"time"
	r "repository"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type Server struct {
	repo       *r.Repository
	log		   *r.CommitLog
	srvConfig  *r.ServerConfig	
	state      *ServerState	
	factory     protocol.MsgFactory
	handler     protocol.ActionHandler
	killch1     chan bool
	killch2     chan bool
}

type ServerState struct {
	done 				bool
	status              protocol.PeerStatus 
	incomings           chan *RequestHandle
	pendings			map[uint64]*RequestHandle    // key : request id
	proposals           map[uint64]*RequestHandle    // key : txnid
	mutex               sync.Mutex
}

type RequestHandle struct {
	request 			 protocol.RequestMsg
	err					 error
	mutex                sync.Mutex
	condVar			    *sync.Cond
}

type ServerCallback interface {
    GetState()	*ServerState
	UpdateStateOnNewProposal(proposal protocol.ProposalMsg) 
	UpdateStateOnCommit(proposal protocol.ProposalMsg) 
}

var gServer *Server = nil

/////////////////////////////////////////////////////////////////////////////
// Server 
/////////////////////////////////////////////////////////////////////////////

//
// Bootstrp
//
func (s *Server) bootstrap() (err error) {

	s.repo, err = r.OpenRepository()
	if err != nil {
		return err
	}

    s.log = r.NewCommitLog(s.repo)	
    s.srvConfig = r.NewServerConfig(s.repo)	
	s.factory = message.NewConcreteMsgFactory()
	s.state = newServerState()
	s.handler = NewServerAction(s)
	s.killch1 = make(chan bool)
	s.killch2 = make(chan bool)

	return nil
}

//
// run election
//
func (s *Server) runElection() (string, error) {

	host := GetHostName()
	peers := GetPeers()
	
	// Create an election site to start leader election.	
	site, err := protocol.CreateElectionSite(host, peers, s.factory, s.handler, s.killch1) 
	if err != nil {
		return "", err
	}
	
	resultCh := make(chan string)
	site.StartElection(resultCh)
	leader := <- resultCh   // blocked until leader is elected
	
	return leader, nil
}

//
// run server (as leader or follower)
//
func (s *Server) runServer(leader string) (err error) {

	host := GetHostName()
	
	// If this host is the leader, then start the leader server.
	// Otherwise, start the followerServer.
	if leader == host {
		s.state.setStatus(protocol.LEADING)
	    _, err = runLeaderServer(host, s.state, s.handler, s.factory, s.killch2)
	} else {
		s.state.setStatus(protocol.FOLLOWING)
		err = runFollowerServer(host, leader, s.state, s.handler, s.factory, s.killch2)
	}
	
	return err 
}

//
// Terminate the Server
//
func (s *Server) Terminate() {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()
	
	if s.state.done {
		return
	}
	
	s.state.done = true

	s.killch1 <- true   // kill election site
	s.killch2 <- true   // kill leader/follower server
}

//
// Check if server is terminated
//
func (s *Server) IsDone() bool {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	return s.state.done	
}

//
// Cleanup internal state upon exit
//
func (s *Server) cleanupState() {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	common.SafeRun("Server.cleanupState()", 
		func() {
			s.repo.Close()
		})		
		
	for {
		request := <- s.state.incomings
		request.err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
		
		common.SafeRun("Server.cleanupState()", 
			func() {
				request.condVar.L.Lock()
				defer request.condVar.L.Unlock()
				request.condVar.Signal()
			})		
	}
	
	for _, request := range s.state.pendings {
		request.err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
		
		common.SafeRun("Server.cleanupState()", 
			func() {
				request.condVar.L.Lock()
				defer request.condVar.L.Unlock()
				request.condVar.Signal()
			})		
	}
	
	for _, request := range s.state.proposals {
		request.err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
		
		common.SafeRun("Server.cleanupState()", 
			func() {
				request.condVar.L.Lock()
				defer request.condVar.L.Unlock()
				request.condVar.Signal()
			})		
	}
}

//
// Run the server until it stop.  Will not attempt to re-run.
//
func runOnce() (int) {

	defer gServer.cleanupState()

	pauseTime := 0
	
	gServer = new(Server)
	
	err := gServer.bootstrap()
	if err != nil {
		pauseTime = 200	
	}
		
	if !gServer.IsDone() {
	
		// runElection() finishes if there is an error, election result is known or
		// it being terminated (killch). Unless being killed explicitly, a goroutine 
		// will continue to run to responds to other peer election request	
		leader, err := gServer.runElection()
		if err != nil {
			pauseTime = 100	
		}
		
		if !gServer.IsDone() {
	
			// runServer() is done if there is an error	or being terminated explicitly (killch)
			 gServer.runServer(leader)
		}
	}
	
	return pauseTime
}

//
// main function
//
func main() {

	repeat := true
	for repeat {
		pauseTime := runOnce() 
		if !gServer.IsDone() {
			if pauseTime > 0 {
				// wait before restart
				timer := time.NewTimer(time.Duration(pauseTime)*time.Millisecond)
				<- timer.C
			}
		} else {
			repeat = false
		}
	}
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
	                     status : protocol.ELECTING,
	                     done : false}
	                       
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
func (s *Server) HandleNewRequest(req protocol.RequestMsg) error {

	if s.IsDone() {
		return common.NewError(common.SERVER_ERROR, "Server is terminated. Cannot process new request.")
	}

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
	s.state.incomings <- handle 

	// This goroutine will wait until the request has been processed.	
	handle.condVar.Wait()

	return handle.err
}

//
// Create a new request handle
//
func newRequestHandle(req protocol.RequestMsg) *RequestHandle {
	handle := &RequestHandle{request : req, err : nil}
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
		s.state.mutex.Lock()
		defer s.state.mutex.Unlock()
	
		// look up the request handle from the pending list and 
		// move it to the proposed list	
		handle, ok := s.state.pendings[reqId]
		if ok {
			delete(s.state.pendings, reqId)				
			s.state.proposals[txnid] = handle 
		} 
	} 
}

//
// Callback when a commit arrives 
//
func (s* Server) UpdateStateOnCommit(proposal protocol.ProposalMsg) {

	txnid := proposal.GetTxnid()
	
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and 
	// notify the waiting goroutine that the request is done.	
	handle, ok := s.state.proposals[txnid]
	if ok {
		delete(s.state.proposals, txnid)
		
		handle.condVar.L.Lock()
		defer handle.condVar.L.Unlock()
		
		handle.condVar.Signal()
	}
}

func (s* Server)  GetState() *ServerState {
	return s.state
}