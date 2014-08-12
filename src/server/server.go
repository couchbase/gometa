package server

import (
	"common"
	"message"
	"protocol"
	r "repository"
	"sync"
	"time"
	"log"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type Server struct {
	repo      		*r.Repository
	log       		*r.CommitLog
	srvConfig 		*r.ServerConfig
	state     		*ServerState
	site      		*protocol.ElectionSite
	factory   		protocol.MsgFactory
	handler   		protocol.ActionHandler
	listener  		*common.PeerListener
	reqListener 	*RequestListener
	skillch   		chan bool
}

type ServerState struct {
	incomings 		chan *RequestHandle

	// mutex protected variables
	mutex     		sync.Mutex
	done      		bool
	status    		protocol.PeerStatus
	pendings  		map[uint64]*RequestHandle // key : request id
	proposals 		map[uint64]*RequestHandle // key : txnid
}

type RequestHandle struct {
	request 		protocol.RequestMsg
	err     		error
	mutex   		sync.Mutex
	condVar 		*sync.Cond
}

type ServerCallback interface {
	GetState() *ServerState
	UpdateStateOnNewProposal(proposal protocol.ProposalMsg)
	UpdateStateOnCommit(proposal protocol.ProposalMsg)
	UpdateWinningEpoch(epoch uint32)
}

var gServer *Server = nil

/////////////////////////////////////////////////////////////////////////////
// Main Function 
/////////////////////////////////////////////////////////////////////////////

func RunServer() error {

	err := NewEnv()
	if err != nil {
		return err	
	}
	
	repeat := true
	for repeat {
		pauseTime := RunOnce()
		if !gServer.IsDone() {
			if pauseTime > 0 {
				// wait before restart
				timer := time.NewTimer(time.Duration(pauseTime) * time.Millisecond)
				<-timer.C
			}
		} else {
			repeat = false
		}
	}
	
	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Server
/////////////////////////////////////////////////////////////////////////////

//
// Bootstrp
//
func (s *Server) bootstrap() (err error) {

	s.state = newServerState()
	
	s.repo, err = r.OpenRepository()
	if err != nil {
		return err
	}

	s.log = r.NewCommitLog(s.repo)
	s.srvConfig = r.NewServerConfig(s.repo)
	s.factory = message.NewConcreteMsgFactory()
	s.handler = NewServerAction(s)
	s.skillch = make(chan bool, 1) // make it buffered to unblock sender

	// Need to start the peer listener before election. A follower may
	// finish its election before a leader finishes its election. Therefore,
	// a follower node can request a connection to the leader node before that
	// node knows it is a leader.  By starting the listener now, it allows the 
	// follower to establish the connection and let the leader handles this 
	// connection at a later time (when it is ready to be a leader).
	s.listener, err = common.StartPeerListener(GetHostTCPAddr())
	if err != nil {
		return common.WrapError(common.SERVER_ERROR, "Fail to start PeerListener.", err)
	}

	// Start a request listener.
	s.reqListener, err = StartRequestListener(GetHostRequestAddr(), s)
	if err != nil {
		return common.WrapError(common.SERVER_ERROR, "Fail to start RequestListener.", err)
	}
	
	s.site = nil
	
	return nil
}

//
// run election
//
func (s *Server) runElection() (leader string, err error) {

	host := GetHostUDPAddr()
	peers := GetPeerUDPAddr()
	
	// Create an election site to start leader election.
	log.Printf("Server.runElection(): Local Server %s start election", host)
	log.Printf("Server.runElection(): Peer in election")
	for _, peer := range peers {
		log.Printf("	peer : %s", peer)
	}
	
	s.site, err = protocol.CreateElectionSite(host, peers, s.factory, s.handler)
	if err != nil {
		return "", err
	}

	resultCh := s.site.StartElection()
	leader, ok := <-resultCh // blocked until leader is elected
	if !ok {
		return "", common.NewError(common.SERVER_ERROR, "Election Fails") 
	}

	return leader, nil
}

//
// run server (as leader or follower)
//
func (s *Server) runServer(leader string) (err error) {

	host := GetHostUDPAddr()

	// If this host is the leader, then start the leader server.
	// Otherwise, start the followerServer.
	if leader == host {
		log.Printf("Server.runServer() : Local Server %s is elected as leader. Leading ...", leader)
		s.state.setStatus(protocol.LEADING)
		err = RunLeaderServer(GetHostTCPAddr(), s.listener, s.state, s.handler, s.factory, s.skillch)
	} else {
		log.Printf("Server.runServer() : Remote Server %s is elected as leader. Following ...", leader)
		s.state.setStatus(protocol.FOLLOWING)
		leaderAddr := findMatchingPeerTCPAddr(leader)
		if len(leaderAddr) == 0 {
			return common.NewError(common.SERVER_ERROR, "Cannot find matching TCP addr for leader " + leader)
		}
		err = RunFollowerServer(GetHostTCPAddr(), leaderAddr, s.state, s.handler, s.factory, s.skillch)
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
	
	s.site.Close()
	s.site = nil

	s.skillch <- true // kill leader/follower server
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
			if s.listener != nil {
				s.listener.Close()
			}
		})
		
	common.SafeRun("Server.cleanupState()",
		func() {
			if s.reqListener != nil {
				s.reqListener.Close()
			}
		})
		
	common.SafeRun("Server.cleanupState()",
		func() {
			if s.repo != nil {
				s.repo.Close()
			}
		})

	common.SafeRun("Server.cleanupState()",
		func() {
			if s.site != nil {
				s.site.Close()
			}
		})
		
	for len(s.state.incomings) > 0 {
		request := <-s.state.incomings
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
func RunOnce() (int) {

	log.Printf("Server.RunOnce() : Start Running Server")
	
	pauseTime := 0
	gServer = new(Server)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in Server.runOnce() : %s\n", r)
		}
	
		common.SafeRun("Server.cleanupState()",
			func() {
				gServer.cleanupState()
			})
	}()

	err := gServer.bootstrap()
	if err != nil {
		pauseTime = 200
	}

	// Check if the server has been terminated explicitly. If so, don't run.
	if !gServer.IsDone() {

		// runElection() finishes if there is an error, election result is known or
		// it being terminated. Unless being killed explicitly, a goroutine
		// will continue to run to responds to other peer election request
		leader, err := gServer.runElection()
		if err != nil {
			log.Printf("Server.RunOnce() : Error Encountered During Election : %s", err.Error())
			pauseTime = 100
		} else {
		
			// Check if the server has been terminated explicitly. If so, don't run.
			if !gServer.IsDone() {
				// runServer() is done if there is an error	or being terminated explicitly (killch)
				err := gServer.runServer(leader)
				if err != nil {
					log.Printf("Server.RunOnce() : Error Encountered From Server : %s", err.Error())
				}
			}
		}
	} else {
		log.Printf("Server.RunOnce(): Server has been terminated explicitly. Terminate.")
	}

	return pauseTime
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
	state := &ServerState{incomings: incomings,
		pendings:  pendings,
		proposals: proposals,
		status:    protocol.ELECTING,
		done:      false}

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
// Request Handle 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new request handle
//
func newRequestHandle(req protocol.RequestMsg) *RequestHandle {
	handle := &RequestHandle{request: req, err: nil}
	handle.condVar = sync.NewCond(&handle.mutex)
	return handle
}

/////////////////////////////////////////////////////////////////////////////
// ServerCallback Interface
/////////////////////////////////////////////////////////////////////////////

//
// Callback when a new proposal arrives
//
func (s *Server) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {

	fid := proposal.GetFid()
	reqId := proposal.GetReqId()
	txnid := proposal.GetTxnid()

	// If this host is the one that sends the request to the leader
	if fid == GetHostTCPAddr() {
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
func (s *Server) UpdateStateOnCommit(proposal protocol.ProposalMsg) {

	log.Printf("Server.UpdateStateOnCommit(): Committing proposal %d, key = %s.", proposal.GetTxnid(), proposal.GetKey())
	txnid := proposal.GetTxnid()

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and
	// notify the waiting goroutine that the request is done.
	handle, ok := s.state.proposals[txnid]
	
	if ok {
		log.Printf("Server.UpdateStateOnCommit(): Notify client for proposal %d", proposal.GetTxnid())
		
		delete(s.state.proposals, txnid)

		handle.condVar.L.Lock()
		defer handle.condVar.L.Unlock()

		handle.condVar.Signal()
	} else {
		log.Printf("Server.UpdateStateOnCommit(): cannot find matching proposal %d to notify client", proposal.GetTxnid())
	} 
}

func (s *Server) GetState() *ServerState {
	return s.state
}

func (s *Server) UpdateWinningEpoch(epoch uint32) {
	s.site.UpdateWinningEpoch(epoch)
}
