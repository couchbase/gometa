 package server

import (
	"common"
	"net"
	"protocol"
	"log"
	"fmt"
)

//////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type FollowerServer struct {
	follower *protocol.Follower
	state    *FollowerState
}

type FollowerState struct {
	serverState *ServerState
}

/////////////////////////////////////////////////////////////////////////////
// FollowerServer - Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new FollowerServer. This is a blocking call until
// the FollowerServer terminates. Make sure the kilch is a buffered
// channel such that if the goroutine running RunFollowerServer goes
// away, the sender won't get blocked. 
//
func RunFollowerServer(naddr string,
	leader string,
	ss *ServerState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch <- chan bool) (err error) {

	// Catch panic at the main entry point for FollowerServer
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in RunFollowerServer() : %s\n", r)
			err = r.(error)
		}
	}()

	// create connection to leader
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		return err
	}
	pipe := common.NewPeerPipe(conn)
	log.Printf("FollowerServer.RunFollowerServer() : Follower %s successfully created TCP connection to leader %s", naddr, leader)

	// close the connection to the leader. If connection is closed,
	// sync proxy and follower will also terminate by err-ing out.  
	// If sync proxy and follower terminates the pipe upon termination, 
	// it is ok to close it again here.
	defer common.SafeRun("FollowerServer.runFollowerServer()",
		func() {
			pipe.Close()
		})

	// start syncrhorniziing with the leader
	success := syncWithLeader(naddr, pipe, handler, factory, killch)

	// run server after synchronization
	if success {
		runFollower(pipe, ss, handler, factory, killch)
		log.Printf("FollowerServer.RunFollowerServer() : Follower Server %s terminate", naddr)
		err = nil
	} else {
		err = common.NewError(common.SERVER_ERROR, fmt.Sprintf("Follower %s fail to synchronized with leader %s", 
				naddr, leader))
	}
	
	return err
}

/////////////////////////////////////////////////////////////////////////////
// FollowerServer - Private Function
/////////////////////////////////////////////////////////////////////////////

//
// Synchronize with the leader.  
//
func syncWithLeader(naddr string,
    pipe *common.PeerPipe,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch <- chan bool) bool {
	
	log.Printf("FollowerServer.syncWithLeader(): Follower %s start synchronization with leader (TCP %s)", 
			naddr, pipe.GetAddr())
	proxy := protocol.NewFollowerSyncProxy(pipe, handler, factory)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate() 

	// This will block until NewFollowerSyncProxy has sychronized with the leader (a bool is pushed to donech)
	select {
	case success := <-donech:
		if success {
			log.Printf("FollowerServer.syncWithLeader(): Follower %s done synchronization with leader (TCP %s)", 
					naddr, pipe.GetAddr())
		}
		return success
	case <-killch:
		// simply return. The pipe will eventually be closed and
		// cause FollowerSyncProxy to err out.
		log.Printf("FollowerServer.syncWithLeader(): Recieve kill singal.  Synchronization with leader (TCP %s) terminated.", 
				pipe.GetAddr())
	}

	return false
}

//
// Run Follower Protocol
//
func runFollower(pipe *common.PeerPipe,
	ss *ServerState,
	handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch <- chan bool) {

	// create the server
	server := new(FollowerServer)

	// create the follower state
	server.state = newFollowerState(ss)

	// Create a follower.  The follower will start a go-rountine, listening to messages coming from leader.
	log.Printf("FollowerServer.runFollower(): Start Follower Protocol")
	server.follower = protocol.NewFollower(protocol.FOLLOWER, pipe, handler, factory)
	donech := server.follower.Start()
	defer server.follower.Terminate()
	
	//start main processing loop
	server.processRequest(handler, factory, killch, donech)
}

//
// main processing loop
//
func (s *FollowerServer) processRequest(handler protocol.ActionHandler,
	factory protocol.MsgFactory,
	killch <-chan bool,
	donech <-chan bool) {
	
	log.Printf("FollowerServer.processRequest(): Ready to process request")
	
	for {
		select {
		case handle, ok := <-s.state.serverState.incomings:
			if ok {
				// move request to pending queue (waiting for proposal)
				s.addPendingRequest(handle)

				// forward the request to the leader
				if !s.follower.ForwardRequest(handle.request) {
					log.Printf("FollowerServer.processRequest(): fail to send client request to leader. Terminate.")
					return
				}
			} else {
				log.Printf("FollowerServer.processRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-killch:
			// server is being explicitly terminated.  Terminate the follower go-rountine as well.
			log.Printf("FollowerServer.processRequest(): receive kill signal. Terminate.")
			return
		case <-donech:
			// follower is done.  Just return.
			log.Printf("FollowerServer.processRequest(): Follower go-routine terminates. Terminate.")
			return
		}
	}
}

//
// Create a new FollowerState
//
func newFollowerState(ss *ServerState) *FollowerState {

	state := &FollowerState{serverState: ss}
	return state
}

/////////////////////////////////////////////////////////////////////////////
// Private Function for protecting shared state
/////////////////////////////////////////////////////////////////////////////

//
// Add Pending Request
//
func (s *FollowerServer) addPendingRequest(handle *RequestHandle) {
	s.state.serverState.mutex.Lock()
	defer s.state.serverState.mutex.Unlock()

	// remember the request
	//log.Printf("FollowerServer.addPendingRequest(): Request Id %d.", handle.request.GetReqId())
	s.state.serverState.pendings[handle.request.GetReqId()] = handle
}
