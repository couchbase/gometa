package server

import (
	"common"
	"protocol"
	"sync"
	"log"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type LeaderServer struct {
	leader       	*protocol.Leader
	listener     	*common.PeerListener
	consentState 	*protocol.ConsentState
	state        	*LeaderState
	handler 	 	 protocol.ActionHandler
	factory 	 	 protocol.MsgFactory
}

type LeaderState struct {
	serverState 	*ServerState

	// mutex protected variable
	mutex   		sync.Mutex
	ready   		bool
	readych         chan bool
	proxies         map[string](chan bool)
}

type ListenerState struct {
	donech			chan bool
	killch			chan bool
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new LeaderServer.  This is a blocking call until the LeaderServer
// termintates.
//
// killch should be unbuffered to ensure the sender won't block 
//
func RunLeaderServer(naddr string,
	listener *common.PeerListener,
	ss *ServerState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch <- chan bool) (err error) {

	log.Printf("LeaderServer.RunLeaderServer(): start leader server %s", naddr)

	// Catch panic at the main entry point for LeaderServer
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in RunLeaderServer() : %s\n", r)
			err = r.(error)
		}
	}()
		
	// create a leader
	leader, err := protocol.NewLeader(naddr, handler, factory)
	if err != nil {
		return err
	}
	defer leader.Terminate()

	// create a ConsentState
	epoch, err := handler.GetAcceptedEpoch()
	if err != nil {
		return err 
	}
	ensembleSize := handler.GetEnsembleSize()
	consentState := protocol.NewConsentState(naddr, epoch, ensembleSize)
	defer consentState.Terminate()

	// create the leader state
	state := newLeaderState(ss)

	// create the server
	server := &LeaderServer{leader: leader,
		listener:     listener,
		consentState: consentState,
		state:        state,
		handler:      handler,
		factory:	  factory}

	// start the listener.  This goroutine would continue to new follower even while
	// it is processing request.
	listenerState := newListenerState()
	go server.listenFollower(listenerState)

	// start the main loop for processing incoming request.  The leader will 
	// process request only after it has received quorum of followers to 
	// synchronized with it.
	err = server.processRequest(killch, listenerState)

	log.Printf("LeaderServer.RunLeaderServer(): leader server %s terminate", naddr)
	
	return err
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Private Function : Discovery Phase (synchronize with follower)
/////////////////////////////////////////////////////////////////////////////

//
// Listen to new connection request from the follower/peer.
// Start a new LeaderSyncProxy to synchronize the state
// between the leader and the peer.
//
func (l *LeaderServer) listenFollower(listenerState *ListenerState) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in LeaderServer.listenFollower() : %s\n", r)
		}
		
		common.SafeRun("LeaderServer.listenFollower()",
			func() {
				l.terminateAllOutstandingProxies()
			})
			
		common.SafeRun("LeaderServer.listenFollower()",
			func() {
				listenerState.donech <- true	
			})
	}()
	
	connCh := l.listener.ConnChannel()
	if connCh == nil {
		// It should not happen unless the listener is closed
		return
	}
	
	for {
		select {
		case conn := <-connCh:
			{
				// There is a new peer connection request from the follower.  Start a proxy to synchronize with the follower.
				// The leader does not proactively connect to follower:
				// 1) The ensemble is stable, but a follower may just reboot and needs to connect to the leader
				// 2) Even if the leader receives votes from the leader, the leader cannot tell for sure that the follower does
				//    not change its vote.  Only if the follower connects, the leader can confirm the follower's alliance. 
				//
				log.Printf("LeaderServer.listenFollower(): Receive connection request from follower %s", conn.RemoteAddr())
				if l.registerOutstandingProxy(conn.RemoteAddr().String()) {
					pipe := common.NewPeerPipe(conn)
					go l.startProxy(pipe)
				} else {
					log.Printf("LeaderServer.listenFollower(): Sync Proxy already running for %s. Ignore new request.", conn.RemoteAddr())
					conn.Close()
				}
			}
		case <- listenerState.killch:
			log.Printf("LeaderServer.listenFollower(): Receive kill signal. Terminate.")
			return
		}
	}
}

//
// Start a LeaderSyncProxy to synchornize the leader
// and follower state.
//
func (l *LeaderServer) startProxy(peer *common.PeerPipe) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in LeaderServer.startProxy() : %s\n", r)
		}
		
		// deregister the proxy with the leader Server upon exit
		l.deregisterOutstandingProxy(peer.GetAddr())
	}()
	
	// create a proxy that will sycnhronize with the peer.   
	log.Printf("LeaderServer.startProxy(): Start synchronization with follower. Peer TCP connection (%s)", peer.GetAddr())
	proxy := protocol.NewLeaderSyncProxy(l.consentState, peer, l.handler, l.factory)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate()
	
	// Get the killch for this proxy
	killch := l.getProxyKillChan(peer.GetAddr())
	if killch == nil {
   		log.Printf("LeaderServer.startProxy(): Cannot find killch for proxy (TCP connection = %s).", peer.GetAddr()) 
   		log.Printf("LeaderServer.startProxy(): Cannot start follower sync.")
   		return
	}
	
	// this go-routine will be blocked until handshake is completed between the
	// leader and the follower.  By then, the leader will also get majority
	// confirmation that it is a leader.
	select {
		case success := <-donech: 
			if success {
				// tell the leader to add this follower for processing request.  If there is a follower running already,
				// AddFollower() will terminate the existing follower instance, and then create a new one.
	    		fid := proxy.GetFid()
				l.leader.AddFollower(fid, peer)
	    		log.Printf("LeaderServer.startProxy(): Synchronization with follower %s done (TCP conn = %s).  Add follower.", 
	    				fid, peer.GetAddr())

				// At this point, the follower has voted this server as the leader.
				// Notify the request processor to start processing new request for this host
				l.notifyReady()
			} else {
				log.Printf("LeaderServer:startProxy(): Leader Fail to synchronization with follower (TCP conn = %s)", peer.GetAddr())
			}
		case <- killch:
			log.Printf("LeaderServer:startProxy(): Sync proxy is killed while synchronizing with follower (TCP conn == %s)", 
					peer.GetAddr())
	} 	
}

//
// Create a new LeaderState
//
func newLeaderState(ss *ServerState) *LeaderState {
	state := &LeaderState{serverState: ss,
							ready: false,
							readych : make(chan bool, 1), // buffered so sender won't wait
							proxies : make(map[string](chan bool))}

	return state
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Private Function : Broadcast phase (handle request)
/////////////////////////////////////////////////////////////////////////////

//
// Goroutine for processing each request one-by-one
//
func (s *LeaderServer) processRequest(killch <-chan bool,
									  listenerState *ListenerState) (err error) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in LeaderServer.startProxy() : %s\n", r)
			err = r.(error)
		}
		
		common.SafeRun("LeaderServer.processRequest()",
			func() {
				listenerState.killch <- true
			})
	}()
	
	// start processing loop after I am being confirmed as a leader (there
	// is a quorum of followers that have sync'ed with me)
	if !s.waitTillReady() {
		return common.NewError(common.ELECTION_ERROR, 
			"LeaderServer.processRequest(): Leader times out waiting for quorum of followers. Terminate")
	}

	// At this point, the leader has gotten a majority of followers to follow, so it
	// can proceed.  It is possible that it may loose quorum of followers. But in that
	// case, the leader will not be able to process any request.
	log.Printf("LeaderServer.processRequest(): Leader Server is ready to proces request")

	// Leader is ready at this time.  This implies that there is a quorum of follower has
	// followed this leader.  Get the change channel to keep track of  number of followers.
	// If the leader no longer has quorum, it needs to let go of its leadership.
	leaderchangech := s.leader.GetEnsembleChangeChannel()
	ensembleSize := s.handler.GetEnsembleSize()
	
	// notify the request processor to start processing new request
	for {
		select {
		case handle, ok := <-s.state.serverState.incomings:
			if ok {
				// de-queue the request
				s.addPendingRequest(handle)

				// create the proposal and forward to the leader
				s.leader.CreateProposal(s.leader.GetFollowerId(), handle.request)
			} else {
				// server shutdown.
				log.Printf("LeaderServer.processRequest(): channel for receiving client request is closed. Terminate.")				
				return nil
			}
		case <-killch:
			// server shutdown 
			log.Printf("LeaderServer.processRequest(): receive kill signal. Stop Client request processing.")
			return nil
		case <-listenerState.donech:
			// listener is down.  Terminate this request processing loop as well.
			log.Printf("LeaderServer.processRequest(): follower listener terminates. Stop client request processing.")
			return nil
		case <-leaderchangech:
			// Listen to any change to the leader's ensemble, and to ensure that the leader maintain majority. 
			numFollowers := s.leader.GetActiveEnsembleSize()
			if numFollowers <= int(ensembleSize/2) {
				// leader looses majority of follower.   
				log.Printf("LeaderServer.processRequest(): leader looses majority of follower. Stop client request processing.")
				return nil
			}
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
	
	if !s.state.ready {
		s.state.ready = true
		s.state.readych <- true
	}
}

//
// Wait for the ready flag to be set.  This is when the leader has gotten
// the quorum of followers to join/sync.
//
func (s *LeaderServer) waitTillReady() bool {

	timeout := time.After(common.LEADER_TIMEOUT * time.Millisecond)
	
	select {
		case <- s.state.readych :
			return true
			
		case <- timeout:
			log.Printf("LeaderServer.waitTillReady(): Leader cannot get quorum of followers to follow before timing out. Termiate.")
			return false
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

/////////////////////////////////////////////////////////////////////////////
// Private Function for mananaging proxies 
/////////////////////////////////////////////////////////////////////////////

// Add Proxy
func (s *LeaderServer) registerOutstandingProxy(key string) bool {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()
	
	_, ok := s.state.proxies[key]
	if !ok {
		killch := make(chan bool, 1) // make it buffered
		s.state.proxies[key] = killch
		return true
	}	
	
	return false
}

//
// Get proxy kill channel
//
func (s *LeaderServer) getProxyKillChan(key string) <-chan bool {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	killch, ok := s.state.proxies[key]
	if ok {
		return killch
	}
	
	return nil
} 

//
// Get proxy kill channel
//
func (s *LeaderServer) deregisterOutstandingProxy(key string) {
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	delete(s.state.proxies, key) 
}

//
// Terminate all proxies 
//
func (s *LeaderServer) terminateAllOutstandingProxies() {
	// TODO: Should copy the proxies and release the mutex
	// before sending to the channels
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	for _, killch := range s.state.proxies {
		killch <- true
	}
}

//
// Create the listener state
//
func newListenerState() *ListenerState {
	return &ListenerState{killch : make(chan bool, 1),   // buffered so sender won't block
					      donech : make(chan bool, 1)}   // buffered so sender won't block
}
