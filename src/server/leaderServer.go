package server

import (
	"protocol"
	"common"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type LeaderServer struct {
	leader   	 *protocol.Leader
	listener 	 *common.PeerListener
	consentState *protocol.ConsentState
	state        *LeaderState
}

type LeaderState struct {
	ready			    bool
	mutex				sync.Mutex
	condVar			    *sync.Cond
	serverState		    *ServerState	
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Discovery Phase (synchronize with follower)
/////////////////////////////////////////////////////////////////////////////

//
// Create a new LeaderServer
//
func newLeaderServer(naddr string, 
                     handler protocol.ActionHandler, 
                     factory protocol.MsgFactory) (*LeaderServer, error) {

	// create a leader
	leader := protocol.NewLeader(naddr, handler, factory)
					           
	// create a ConsentState 
	epoch, err := handler.GetAcceptedEpoch()
	if err != nil {
		return nil, err
	}
	ensembleSize := uint64(len(GetPeers()))
	consentState := protocol.NewConsentState(naddr, epoch, ensembleSize) 
	
	// create the leader state
	state := newLeaderState()
					
	// create a listener to listen to connection from the peer/follower				           
	listener, err := common.StartPeerListener(naddr) 
	if err != nil {
		return nil, common.NewError(common.SERVER_ERROR, "Fail to start PeerListener.")	
	}
	
	// create the server					 
	server := &LeaderServer{leader : leader,
	                        listener : listener,
	                        consentState: consentState,
	                        state : state}

	// start a go-routine for processing incoming request	
	go server.processRequest(handler, factory)

	// start the listener	
	go server.listenFollower(listener, consentState, handler, factory)
	
	return server, nil
}
	
//
// Listen to new connection request from the follower/peer.
// Start a new LeaderSyncProxy to synchronize the state
// between the leader and the peer.  
//
func (l *LeaderServer) listenFollower(listener *common.PeerListener,
						              state *protocol.ConsentState,
						              handler protocol.ActionHandler,
						              factory protocol.MsgFactory) {
	
	connCh := listener.ConnChannel()
	if connCh == nil {
		// It should not happen unless the listener is closed
		return
	}

	for {
		select {
			case conn := <- connCh : {
				// There is a new peer connection request
				// TODO: Check if it is not a duplicate
				pipe, err := common.NewPeerPipe(conn)
				if err != nil {
					// TODO : return error
				}
				
				go l.startProxy(pipe, state, handler, factory)	
			}
			// TODO: handle shutdown	
			default : {}
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
	proxy := protocol.NewLeaderSyncProxy(state,  peer, handler, factory) 
	donech := make(chan bool)
	proxy.Start(donech)
	
	// this go-routine will be blocked until handshake is completed between the
	// leader and the follower.  By then, the leader will also get majority 
	// confirmation that it is a leader.
	<- donech
	
	l.state.mutex.Lock()
	defer l.state.mutex.Unlock()
	
	// tell the leader to add this follower for processing request
	l.leader.AddFollower(peer)

	// At this point, the follower has voted this server as the leader.
	// Notify the request processor to start processing new request for this host 
	l.state.ready = true
	l.state.condVar.Signal()
}

//
// Create a new LeaderState
//
func newLeaderState() *LeaderState {
	serverState := newServerState()
	serverState.setStatus(protocol.LEADING)
	
	state := &LeaderState{serverState : serverState,
	                     ready : false}
	
	state.condVar = sync.NewCond(&state.mutex)	
	return state
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - Broadcast phase (handle request) 
/////////////////////////////////////////////////////////////////////////////

//
// Goroutine for processing each request one-by-one
//
func (s *LeaderServer) processRequest(handler   protocol.ActionHandler, 
					                    factory   protocol.MsgFactory) {
					                    
	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()
	
	if !s.state.ready {
		s.state.condVar.Wait()
	}
	
	// notify the request processor to start processing new request
	for {
		// TODO : handle shutdownd
		
		// de-queue the request
		handle := <- s.state.serverState.incomings
		
		s.state.serverState.mutex.Lock()
		defer s.state.serverState.mutex.Unlock()
		
		// remember the request 		
		s.state.serverState.pendings[handle.request.GetReqId()] = handle
		
		// create the proposal and forward to the leader 
		s.leader.CreateProposal(GetHostName(), handle.request)
	}
}

/////////////////////////////////////////////////////////////////////////////
// LeaderServer - StateProvider Interface 
/////////////////////////////////////////////////////////////////////////////

func (s *LeaderServer) GetState() *ServerState {
	return s.state.serverState
}
