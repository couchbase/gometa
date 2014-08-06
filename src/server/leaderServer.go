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

	// start the listener
	killch2 := make(chan bool)
	go server.listenFollower(listener, consentState, handler, factory, killch2)

	// start the main loop for processing incoming request
	server.processRequest(handler, factory, killch, killch2)

	return nil
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

	for {
		select {
		case conn := <-connCh:
			{
				// There is a new peer connection request
				// TODO: Check if it is not a duplicate
				log.Printf("LeaderServer.listenFollower(): Receive connection request from follower %s", conn.RemoteAddr())
				pipe := common.NewPeerPipe(conn)
				go l.startProxy(pipe, state, handler, factory)
			}
		case <-killch:
			l.leader.Terminate()
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
	factory protocol.MsgFactory) {

	// create a proxy that will sycnhronize with the peer
	log.Printf("LeaderServer.listenFollower(): Start synchronization with follower %s", peer.GetAddr())
	proxy := protocol.NewLeaderSyncProxy(state, peer, handler, factory)
	donech := make(chan bool)
	proxy.Start(donech)

	// this go-routine will be blocked until handshake is completed between the
	// leader and the follower.  By then, the leader will also get majority
	// confirmation that it is a leader.
	success := <-donech

	if success {

		// tell the leader to add this follower for processing request
	    log.Printf("LeaderServer.listenFollower(): Synchronization with follower %s done.  Add follower.", peer.GetAddr())
		l.leader.AddFollower(peer)

		// At this point, the follower has voted this server as the leader.
		// Notify the request processor to start processing new request for this host
		l.notifyReady()
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
	lisKillch chan bool) {

	// start processing loop after I am being confirmed as a leader (there
	// is a quorum of followers that have sync'ed with me)
	s.waitTillReady()

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
				return
			}

		case <-killch:
			// server shutdown.
			lisKillch <- true
			return
		}
	}
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
// Add Pending Request
//
func (s *LeaderServer) addPendingRequest(handle *RequestHandle) {
	s.state.serverState.mutex.Lock()
	defer s.state.serverState.mutex.Unlock()

	// remember the request
	s.state.serverState.pendings[handle.request.GetReqId()] = handle
}
