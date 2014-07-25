package server

import (
	"protocol"
	"common"
	"net"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type FollowerServer struct {
	follower     *protocol.Follower
	state        *FollowerState
}

type FollowerState struct {
	serverState			*ServerState
}

/////////////////////////////////////////////////////////////////////////////
// FollowerServer 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new FollowerServer
//
func runFollowerServer(naddr string, 
					   leader string,
					   ss *ServerState,
                       handler protocol.ActionHandler, 
                       factory protocol.MsgFactory,
                       killch chan bool) error {
                       
	// create connection to leader
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		return nil, err	
	}
	pipe, err := common.NewPeerPipe(conn)
	if err != nil {
		return nil, err
	}

	// close the connection to the leader. If connection is closed,
	// sync proxy and follower will terminate by err-ing out.
	defer common.SafeRun("FollowerServer.runFollowerServer()", 
		func() {
		 	pipe.Close()	
		})	

	// start syncrhorniziing with the leader
	success := syncWithLeader(pipe, handler, factory, killch)
	
	// run server after synchronization
	if success {
		runFollower(pipe, ss, handler, factory, killch)
	}

	return nil
}

//
// Synchronize with the leader
//
func syncWithLeader(pipe      *common.PeerPipe,
					handler   protocol.ActionHandler, 
	                factory   protocol.MsgFactory,
	                killch    chan bool) bool
	
	proxy := protocol.NewFollowerSyncProxy(pipe, handler, factory)                    	 
	
	donech := make(chan bool)
	proxy.Start(donech)
	
	// This goroutine will block until NewFollowerSyncProxy has sychronized with
	// the leader (a bool is pushed to donech)
	select {
		case success := <- donech :
			return success
		case <- killch :
			// simply return. The pipe will eventually be closed and
			// cause FollowerSyncProxy to err out.
			return false
	}

	return false
}

//
// Run Follower Protocol 
//
func runFollower(pipe      *common.PeerPipe,
			   ss        *ServerState
			   handler   protocol.ActionHandler, 
	           factory   protocol.MsgFactory,
	           killch    chan bool) {
	           
	// create the server					 
	server := new(FollowerServer)
	
	// create the follower state
	server.state = newFollowerState(ss) 
	                    
	// Create a follower.  The follower will start a go-rountine, listening to messages coming from leader. 
	donech := make(chan bool)
	server.follower = protocol.StartFollower(protocol.FOLLOWER, pipe, handler, factory, donech)
	
	//start main processing loop 
	server.processRequest(handler, factory, killch, donech)
}

//
// Create a new FollowerState
//
func newFollowerState(ss *ServerState) *FollowerState {
	
	state := &FollowerState{serverState : ss}
	return state                       
}

//
// main processing loop 
//
func (s *FollowerServer) processRequest(handler   	protocol.ActionHandler, 
					                    factory 	protocol.MsgFactory,
					                    killch 		chan bool,
					                    donech 		chan bool) {
	for {
		select {
		case handle, ok := <- s.state.serverState.incomings :
			// de-queue the request
			if ok {
				s.state.serverState.mutex.Lock()
				defer s.state.serverState.mutex.Unlock()
		
				// remember the request 		
				s.state.serverState.pendings[handle.request.GetReqId()] = handle
		
				// forward the request to the leader
				if !s.follower.ForwardRequest(handle.request) {
					// return if fail to connect to leader
					return	
				}
			} else {
				return 
			}
		}
		case <- killch:
			// server is being explicitly terminated, simply return.
			// The pipe will eventually be closed and cause Follower to err out.
			return
		case <- done:
			// follower is done.  Just return.
		 	return	
	}
}