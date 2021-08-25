// @author Couchbase <info@couchbase.com>
// @copyright 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"fmt"
	"net"

	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/security"
)

//////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type FollowerServer struct {
	follower *Follower
	state    *FollowerState
}

type FollowerState struct {
	requestMgr RequestMgr
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
	ss RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool) (err error) {

	// Catch panic at the main entry point for FollowerServer
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in RunFollowerServer() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
			err = r.(error)
		} else {
			log.Current.Debugf("%s", "RunFollowerServer terminates.")
			log.Current.Tracef(log.Current.StackTrace())
		}
	}()

	// create connection to leader
	conn, err := createConnection(leader)
	if err != nil {
		return err
	}

	pipe := common.NewPeerPipe(conn)
	log.Current.Debugf("FollowerServer.RunFollowerServer() : Follower %s successfully "+
		"created TCP connection to leader %s, local address %s", naddr, leader, conn.LocalAddr())

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
		log.Current.Debugf("FollowerServer.RunFollowerServer() : Follower Server %s terminate", naddr)
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
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool) bool {

	log.Current.Debugf("FollowerServer.syncWithLeader(): Follower %s start synchronization with leader (TCP %s)",
		naddr, pipe.GetAddr())
	proxy := NewFollowerSyncProxy(pipe, handler, factory, true)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate()

	// This will block until NewFollowerSyncProxy has sychronized with the leader (a bool is pushed to donech)
	select {
	case success := <-donech:
		if success {
			log.Current.Debugf("FollowerServer.syncWithLeader(): Follower %s done synchronization with leader (TCP %s)",
				naddr, pipe.GetAddr())
		}
		return success
	case <-killch:
		// simply return. The pipe will eventually be closed and
		// cause FollowerSyncProxy to err out.
		log.Current.Debugf("FollowerServer.syncWithLeader(): Recieve kill singal.  Synchronization with leader (TCP %s) terminated.",
			pipe.GetAddr())
	}

	return false
}

//
// Run Follower Protocol
//
func runFollower(pipe *common.PeerPipe,
	ss RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool) {

	// create the server
	server := new(FollowerServer)

	// create the follower state
	server.state = newFollowerState(ss)

	// Create a follower.  The follower will start a go-rountine, listening to messages coming from leader.
	log.Current.Debugf("FollowerServer.runFollower(): Start Follower Protocol")
	server.follower = NewFollower(FOLLOWER, pipe, handler, factory)
	donech := server.follower.Start()
	defer server.follower.Terminate()

	//start main processing loop
	server.processRequest(handler, factory, killch, donech)
}

//
// main processing loop
//
func (s *FollowerServer) processRequest(handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	donech <-chan bool) {

	log.Current.Debugf("FollowerServer.processRequest(): Ready to process request")

	incomings := s.state.requestMgr.GetRequestChannel()
	for {
		select {
		case handle, ok := <-incomings:
			if ok {
				// move request to pending queue (waiting for proposal)
				s.state.requestMgr.AddPendingRequest(handle)

				// forward the request to the leader
				if !s.follower.ForwardRequest(handle.Request) {
					log.Current.Errorf("FollowerServer.processRequest(): fail to send client request to leader. Terminate.")
					return
				}
			} else {
				log.Current.Debugf("FollowerServer.processRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-killch:
			// server is being explicitly terminated.  Terminate the follower go-rountine as well.
			log.Current.Debugf("FollowerServer.processRequest(): receive kill signal. Terminate.")
			return
		case <-donech:
			// follower is done.  Just return.
			log.Current.Debugf("FollowerServer.processRequest(): Follower go-routine terminates. Terminate.")
			return
		}
	}
}

//
// Create a new FollowerState
//
func newFollowerState(ss RequestMgr) *FollowerState {

	state := &FollowerState{requestMgr: ss}
	return state
}

//
// Create a connection
//
func createConnection(leader string) (net.Conn, error) {

	c, err := security.MakeTCPConn(leader)
	if err != nil {
		return nil, err
	}

	c.SetKeepAlive(true)
	c.SetKeepAlivePeriod(common.TCP_KEEP_ALIVE_PERIOD)

	host, port, err := net.SplitHostPort(leader)
	if err != nil {
		return nil, err
	}

	conn, err := security.SecureConn(c, host, port)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
