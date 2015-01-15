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

package server

import (
	"errors"
	"github.com/couchbase/gometa/action"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	r "github.com/couchbase/gometa/repository"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type Server struct {
	repo        *r.Repository
	log         *r.CommitLog
	srvConfig   *r.ServerConfig
	txn         *common.TxnState
	state       *ServerState
	site        *protocol.ElectionSite
	factory     protocol.MsgFactory
	handler     *action.ServerAction
	listener    *common.PeerListener
	reqListener *RequestListener
	skillch     chan bool
}

type ServerState struct {
	incomings chan *protocol.RequestHandle

	// mutex protected variables
	mutex     sync.Mutex
	done      bool
	status    protocol.PeerStatus
	pendings  map[uint64]*protocol.RequestHandle       // key : request id
	proposals map[common.Txnid]*protocol.RequestHandle // key : txnid
}

var gServer *Server = nil

/////////////////////////////////////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////////////////////////////////////

func RunServer(config string) error {

	err := NewEnv(config)
	if err != nil {
		return err
	}

	for {
		pauseTime := RunOnce()
		if !gServer.IsDone() {
			if pauseTime > 0 {
				// wait before restart
				time.Sleep(time.Duration(pauseTime) * time.Millisecond)
			}
		} else {
			break
		}
	}

	return nil
}

func (s *Server) GetValue(key string) ([]byte, error) {

	return s.handler.Get(key)
}

/////////////////////////////////////////////////////////////////////////////
// Server
/////////////////////////////////////////////////////////////////////////////

//
// Bootstrp
//
func (s *Server) bootstrap() (err error) {

	// Initialize server state
	s.state = newServerState()

	// Initialize repository service
	s.repo, err = r.OpenRepository()
	if err != nil {
		return err
	}
	s.log = r.NewCommitLog(s.repo)
	s.srvConfig = r.NewServerConfig(s.repo)

	// Create and initialize new txn state.
	s.txn = common.NewTxnState()

	// initialize the current transaction id to the lastLoggedTxid.  This
	// is the txid that this node has seen so far.  If this node becomes
	// the leader, a new epoch will be used and new current txid will
	// be generated. So no need to initialize the epoch at this point.
	lastLoggedTxid, err := s.srvConfig.GetLastLoggedTxnId()
	if err != nil {
		return err
	}
	s.txn.InitCurrentTxnid(common.Txnid(lastLoggedTxid))

	// Initialize various callback facility for leader election and
	// voting protocol.
	s.factory = message.NewConcreteMsgFactory()
	s.handler = action.NewServerAction(s.repo, s.log, s.srvConfig, s, s.txn, s.factory, s)
	s.skillch = make(chan bool, 1) // make it buffered to unblock sender
	s.site = nil

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

	s.site, err = protocol.CreateElectionSite(host, peers, s.factory, s.handler, false)
	if err != nil {
		return "", err
	}

	resultCh := s.site.StartElection()
	if resultCh == nil {
		return "", common.NewError(common.SERVER_ERROR, "Election Site is in progress or is closed.")
	}

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
		err = protocol.RunLeaderServer(GetHostTCPAddr(), s.listener, s.state, s.handler, s.factory, s.skillch)
	} else {
		log.Printf("Server.runServer() : Remote Server %s is elected as leader. Following ...", leader)
		s.state.setStatus(protocol.FOLLOWING)
		leaderAddr := findMatchingPeerTCPAddr(leader)
		if len(leaderAddr) == 0 {
			return common.NewError(common.SERVER_ERROR, "Cannot find matching TCP addr for leader "+leader)
		}
		err = protocol.RunFollowerServer(GetHostTCPAddr(), leaderAddr, s.state, s.handler, s.factory, s.skillch)
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
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("Server.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.pendings {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("Server.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.proposals {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("Server.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}
}

//
// Run the server until it stop.  Will not attempt to re-run.
//
func RunOnce() int {

	log.Printf("Server.RunOnce() : Start Running Server")

	pauseTime := 0
	gServer = new(Server)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in Server.runOnce() : %s\n", r)
		}

		log.Printf("RunOnce() terminates : Diagnostic Stack ...")
		log.Printf("%s", debug.Stack())

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
// QuorumVerifier
/////////////////////////////////////////////////////////////////////////////

func (s *Server) HasQuorum(count int) bool {
	ensembleSz := s.handler.GetEnsembleSize()
	return count > int(ensembleSz/2)
}

/////////////////////////////////////////////////////////////////////////////
// ServerState
/////////////////////////////////////////////////////////////////////////////

//
// Create a new ServerState
//
func newServerState() *ServerState {

	incomings := make(chan *protocol.RequestHandle, common.MAX_PROPOSALS)
	pendings := make(map[uint64]*protocol.RequestHandle)
	proposals := make(map[common.Txnid]*protocol.RequestHandle)
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

func (s *ServerState) AddPendingRequest(handle *protocol.RequestHandle) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// remember the request
	s.pendings[handle.Request.GetReqId()] = handle
}

func (s *ServerState) GetRequestChannel() <-chan *protocol.RequestHandle {

	return (<-chan *protocol.RequestHandle)(s.incomings)
}

/////////////////////////////////////////////////////////////////////////////
// Request Handle
/////////////////////////////////////////////////////////////////////////////

//
// Create a new request handle
//
func newRequestHandle(req protocol.RequestMsg) *protocol.RequestHandle {
	handle := &protocol.RequestHandle{Request: req, Err: nil}
	handle.CondVar = sync.NewCond(&handle.Mutex)
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
	if fid == s.handler.GetFollowerId() {
		s.state.mutex.Lock()
		defer s.state.mutex.Unlock()

		// look up the request handle from the pending list and
		// move it to the proposed list
		handle, ok := s.state.pendings[reqId]
		if ok {
			delete(s.state.pendings, reqId)
			s.state.proposals[common.Txnid(txnid)] = handle
		}
	}
}

func (s *Server) UpdateStateOnRespond(fid string, reqId uint64, err string) {

	// If this host is the one that sends the request to the leader
	if fid == s.handler.GetFollowerId() {
		s.state.mutex.Lock()
		defer s.state.mutex.Unlock()

		// look up the request handle from the pending list and
		// move it to the proposed list
		handle, ok := s.state.pendings[reqId]
		if ok {
			delete(s.state.pendings, reqId)

			handle.CondVar.L.Lock()
			defer handle.CondVar.L.Unlock()

			if len(err) != 0 {
				handle.Err = errors.New(err)
			}

			handle.CondVar.Signal()
		}
	}
}

//
// Callback when a commit arrives
//
func (s *Server) UpdateStateOnCommit(txnid common.Txnid, key string) {

	log.Printf("Server.UpdateStateOnCommit(): Committing proposal %d key %s.", txnid, key)

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and
	// notify the waiting goroutine that the request is done.
	handle, ok := s.state.proposals[txnid]

	if ok {
		log.Printf("Server.UpdateStateOnCommit(): Notify client for proposal %d", txnid)

		delete(s.state.proposals, txnid)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		handle.CondVar.Signal()
	}
}

func (s *Server) GetStatus() protocol.PeerStatus {
	return s.state.getStatus()
}

func (s *Server) UpdateWinningEpoch(epoch uint32) {
	// update the election site with the new epoch, such that
	// for new incoming vote, the server can reply with the
	// new and correct epoch
	s.site.UpdateWinningEpoch(epoch)

	// any new tnxid from now on will use the new epoch
	s.txn.SetEpoch(epoch)
}

func (s *Server) GetPeerUDPAddr() []string {
	return GetPeerUDPAddr()
}

func (s *Server) GetHostTCPAddr() string {
	return GetHostTCPAddr()
}

func (s *Server) GetEnsembleSize() uint64 {
	return uint64(len(GetPeerUDPAddr())) + 1 // including myself
}

func (s *Server) GetFollowerId() string {
	return GetHostTCPAddr()
}
