package server

import (
	"common"
	"protocol"
	"sync"
	"log"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type LeaderServer struct {
	leader       *protocol.Leader
	listener     *common.PeerListener
	consentState *protocol.ConsentState
	state        *LeaderState
}

type LeaderState struct {
	serverState *ServerState

	// mutex protected variable
	mutex   sync.Mutex
	condVar *sync.Cond
	ready   bool
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Discovery Phase (synchronize with follower)
/////////////////////////////////////////////////////////////////////////////

//
// Create a new LeaderServer.  This is a blocking call until the LeaderServer
// termintates.
//
func RunLeaderServer(naddr string,
    listener *common.PeerListener,
	ss *ServerState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch chan bool) error {

	log.Printf("LeaderServer.RunLeaderServer(): start leader server %s", naddr)
	
	// create a leader
	leader := protocol.NewLeader(naddr, handler, factory)

	// create a ConsentState
	epoch, err := handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	ensembleSize := uint64(len(GetPeerUDPAddr())) + 1 // include the leader itself in the ensemble size 
	consentState := protocol.NewConsentState(naddr, epoch, ensembleSize)

	// create the leader state
	state := newLeaderState(ss)

	// create the server
	server := &LeaderServer{leader: leader,
		listener:     listener,
		consentState: consentState,
		state:        state}

	// start the listener.  This goroutine would continue to new follower even while
	// it is processing request.
	killch2 := make(chan bool)
	go server.listenFollower(listener, consentState, handler, factory, killch2)

	// start the main loop for processing incoming request.  The leader will 
	// process request only after it has received quorum of followers to 
	// synchronized with it.
	err = server.processRequest(handler, factory, killch, killch2)

	log.Printf("LeaderServer.RunLeaderServer(): leader server %s terminate", naddr)
	
	return err
}

//
// Listen to new connection request from the follower/peer.
// Start a new LeaderSyncProxy to synchronize the state
// between the leader and the peer.
//
func (l *LeaderServer) listenFollower(listener *common.PeerListener,
	state *protocol.ConsentState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch chan bool) {
	
	connCh := listener.ConnChannel()
	if connCh == nil {
		// It should not happen unless the listener is closed
		return
	}
	
	ensembleSize := uint64(len(GetPeerUDPAddr())) + 1 // include the leader itself in the ensemble size 
	killchs := make([] chan bool, 0, ensembleSize)

	for {
		select {
		case conn := <-connCh:
			{
				// There is a new peer connection request
				// TODO: Check if it is not a duplicate
				log.Printf("LeaderServer.listenFollower(): Receive connection request from follower %s", conn.RemoteAddr())
				pipe := common.NewPeerPipe(conn)
				killch2 := make(chan bool)
				killchs = append(killchs, killch2)
				go l.startProxy(pipe, state, handler, factory, killch2)
			}
		case <-killch:
			log.Printf("LeaderServer.listenFollower(): Receive kill signal. Terminate.")
			for _, killch2 := range killchs {
				killch2 <- true
			}
			return
		}
	}
}

//
// Start a LeaderSyncProxy to synchornize the leader
// and follower state.
//
func (l *LeaderServer) startProxy(peer *common.PeerPipe,
	state *protocol.ConsentState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch chan bool) {

	// create a proxy that will sycnhronize with the peer
	log.Printf("LeaderServer.startProxy(): Start synchronization with follower %s", peer.GetAddr())
	proxy := protocol.NewLeaderSyncProxy(state, peer, handler, factory)
	donech := proxy.GetDoneChannel()
	go proxy.Start()

	// this go-routine will be blocked until handshake is completed between the
	// leader and the follower.  By then, the leader will also get majority
	// confirmation that it is a leader.
	select {
		case success := <-donech: 
			if success {
				// tell the leader to add this follower for processing request
	    		log.Printf("LeaderServer.startProxy(): Synchronization with follower %s done.  Add follower.", peer.GetAddr())
				l.leader.AddFollower(peer)

				// At this point, the follower has voted this server as the leader.
				// Notify the request processor to start processing new request for this host
				l.notifyReady()
			} else {
				log.Printf("LeaderServer:startProxy(): Leader Fail to synchronization with follower %s", peer.GetAddr())
			}
		case <- killch:
			log.Printf("LeaderServer:startProxy(): Sync proxy is killed while synchronizing with follower %s", peer.GetAddr())
			proxy.Terminate()
	} 	
}

//
// Create a new LeaderState
//
func newLeaderState(ss *ServerState) *LeaderState {
	state := &LeaderState{serverState: ss,
		ready: false}

	state.condVar = sync.NewCond(&state.mutex)
	return state
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Broadcast phase (handle request)
/////////////////////////////////////////////////////////////////////////////

//
// Goroutine for processing each request one-by-one
//
func (s *LeaderServer) processRequest(handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch chan bool,
	lisKillch chan bool) error {

	// start processing loop after I am being confirmed as a leader (there
	// is a quorum of followers that have sync'ed with me)
	s.waitTillReady()
	if !s.isReady() {
		return common.NewError(common.ELECTION_ERROR, 
			"LeaderServer.processRequest(): Server still not ready. Terminate")
	}

	// At this point, the leader has gotten a majority of followers to follow, so it
	// can proceed.  It is possible that it may loose quorum of followers. But in that
	// case, the leader will not be able to process any request.
	log.Printf("LeaderServer.processRequest(): Leader Server is ready to proces request")
	
	// notify the request processor to start processing new request
	for {
		select {
		case handle, ok := <-s.state.serverState.incomings:
			if ok {
				// de-queue the request
				s.addPendingRequest(handle)

				// create the proposal and forward to the leader
				// TODO: This will send to every follower asynchronously
				s.leader.CreateProposal(GetHostTCPAddr(), handle.request)
			} else {
				// server shutdown.
				lisKillch <- true
				s.leader.Terminate()
				return nil
			}

		case <-killch:
			// server shutdown 
			lisKillch <- true
			s.leader.Terminate()
			return nil
		}
	}
	
	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Private Function for protecting shared state
/////////////////////////////////////////////////////////////////////////////

//
// Notify when server is ready
//
func (s *LeaderServer) notifyReady() {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	s.state.ready = true
	s.state.condVar.Signal()
}

//
// Wait for the ready flag to be set.  This is when the leader has gotten
// the quorum of followers to join/sync.
//
func (s *LeaderServer) waitTillReady() {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	if !s.state.ready {
		s.state.condVar.Wait()
	}
}

//
// Tell if the server is ready
//
func (s *LeaderServer) isReady() bool {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()
	
	return s.state.ready
}

//
// Add Pending Request
//
func (s *LeaderServer) addPendingRequest(handle *RequestHandle) {
	s.state.serverState.mutex.Lock()
	defer s.state.serverState.mutex.Unlock()

	// remember the request
	s.state.serverState.pendings[handle.request.GetReqId()] = handle
}
