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
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type EmbeddedServer struct {
	msgAddr    string
	repo       *r.Repository
	log        r.CommitLogger
	srvConfig  *r.ServerConfig
	txn        *common.TxnState
	state      *ServerState
	factory    protocol.MsgFactory
	handler    *action.ServerAction
	notifier   action.EventNotifier
	reqHandler protocol.CustomRequestHandler
	listener   *common.PeerListener
	skillch    chan bool
}

/////////////////////////////////////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////////////////////////////////////

func RunEmbeddedServer(msgAddr string) (*EmbeddedServer, error) {

	return RunEmbeddedServerWithNotifier(msgAddr, nil)
}

func RunEmbeddedServerWithNotifier(msgAddr string, notifier action.EventNotifier) (*EmbeddedServer, error) {

	return RunEmbeddedServerWithCustomHandler(msgAddr, notifier, nil)
}

func RunEmbeddedServerWithCustomHandler(msgAddr string, notifier action.EventNotifier, reqHandler protocol.CustomRequestHandler) (*EmbeddedServer, error) {

	server := new(EmbeddedServer)
	server.msgAddr = msgAddr
	server.notifier = notifier
	server.reqHandler = reqHandler

	if err := server.bootstrap(); err != nil {
		log.Printf("EmbeddedServer.boostrap: error : %v\n", err)
		return nil, err
	}

	go server.run()

	return server, nil
}

//
// Terminate the Server
//
func (s *EmbeddedServer) Terminate() {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	if s.state.done {
		return
	}

	s.state.done = true

	s.skillch <- true // kill leader/follower server
}

//
// Check if server is terminated
//
func (s *EmbeddedServer) IsDone() bool {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	return s.state.done
}

//
// Retrieve value
//
func (s *EmbeddedServer) GetValue(key string) ([]byte, error) {

	return s.handler.Get(key)
}

//
// Set value
//
func (s *EmbeddedServer) SetValue(key string, value []byte) {
	s.Set(key, value)
}

//
// Delete value
//
func (s *EmbeddedServer) DeleteValue(key string) {
	s.Delete(key)
}

//
// Set value
//
func (s *EmbeddedServer) Set(key string, value []byte) error {

	id := uint64(time.Now().UnixNano())

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_SET),
		key,
		value)

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Printf("Handing new request to server. Key %s", key)
	s.state.incomings <- handle

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Printf("Receive Response for request. Key %s", key)

	return handle.Err
}

//
// Set value
//
func (s *EmbeddedServer) CustomSet(key string, value []byte) error {

	id := uint64(time.Now().UnixNano())

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_CUSTOM_SET),
		key,
		value)

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Printf("Handing new request to server. Key %s", key)
	s.state.incomings <- handle

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Printf("Receive Response for request. Key %s", key)

	return handle.Err
}

//
// Delete value
//
func (s *EmbeddedServer) Delete(key string) error {

	id := uint64(time.Now().UnixNano())

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_DELETE),
		key,
		[]byte(""))

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Printf("Handing new request to server. Key %s", key)
	s.state.incomings <- handle

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Printf("Receive Response for request. Key %s", key)

	return handle.Err
}

//
// Create a new iterator
//
func (s *EmbeddedServer) GetIterator(startKey, endKey string) (*r.RepoIterator, error) {

	return s.repo.NewIterator(startKey, endKey)
}

func (s *EmbeddedServer) SetConfigValue(key string, value string) error {
	return s.srvConfig.LogStr(key, value)
}

func (s *EmbeddedServer) DeleteConfigValue(key string) error {
	return s.srvConfig.Delete(key)
}

func (s *EmbeddedServer) GetConfigValue(key string) (string, error) {
	return s.srvConfig.GetStr(key)
}

/////////////////////////////////////////////////////////////////////////////
// Server
/////////////////////////////////////////////////////////////////////////////

//
// Bootstrp
//
func (s *EmbeddedServer) bootstrap() (err error) {

	defer func() {
		r := recover()
		if r != nil {
			log.Printf("panic in EmbeddedServer.bootstrap() : %s\n", r)
			log.Printf("%s", debug.Stack())
		}

		if err != nil || r != nil {
			common.SafeRun("EmbeddedServer.bootstrap()",
				func() {
					s.cleanupState()
				})
		}
	}()

	// Initialize server state
	s.state = newServerState()

	// Initialize repository service
	s.repo, err = r.OpenRepository()
	if err != nil {
		return err
	}
	s.log = r.NewTransientCommitLog(s.repo)
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
	s.handler = action.NewServerActionWithNotifier(s.repo, s.log, s.srvConfig, s, s.notifier, s.txn, s.factory, s)
	s.skillch = make(chan bool, 1) // make it buffered to unblock sender

	// Need to start the peer listener before election. A follower may
	// finish its election before a leader finishes its election. Therefore,
	// a follower node can request a connection to the leader node before that
	// node knows it is a leader.  By starting the listener now, it allows the
	// follower to establish the connection and let the leader handles this
	// connection at a later time (when it is ready to be a leader).
	s.listener, err = common.StartPeerListener(s.msgAddr)
	if err != nil {
		err = common.WrapError(common.SERVER_ERROR, "Fail to start PeerListener. err = %v", err)
		return
	}

	return nil
}

func (s *EmbeddedServer) run() {

	for {
		s.runOnce()
		if !s.IsDone() {
			time.Sleep(time.Duration(200) * time.Millisecond)

			if !s.IsDone() {
				if err := s.bootstrap(); err != nil {
					log.Printf("EmbeddedServer.boostrap: error : %v\n", err)
				}
			}
		} else {
			break
		}
	}
}

//
// Cleanup internal state upon exit
//
func (s *EmbeddedServer) cleanupState() {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	common.SafeRun("EmbeddedServer.cleanupState()",
		func() {
			if s.listener != nil {
				s.listener.Close()
			}
		})

	common.SafeRun("EmbeddedServer.cleanupState()",
		func() {
			if s.repo != nil {
				s.repo.Close()
			}
		})

	for len(s.state.incomings) > 0 {
		request := <-s.state.incomings
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.pendings {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.proposals {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanupState()",
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
func (s *EmbeddedServer) runOnce() {

	log.Printf("EmbeddedServer.runOnce() : Start Running Server")

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in EmbeddedServer.runOnce() : %v\n", r)
			log.Printf("Diagnostic Stack ...")
			log.Printf("%s", debug.Stack())
		}

		common.SafeRun("EmbeddedServer.cleanupState()",
			func() {
				s.cleanupState()
			})
	}()

	// Check if the server has been terminated explicitly. If so, don't run.
	if !s.IsDone() {

		// runServer() is done if there is an error	or being terminated explicitly (killch)
		s.state.setStatus(protocol.LEADING)
		if err := protocol.RunLeaderServerWithCustomHandler(
			s.msgAddr, s.listener, s.state, s.handler, s.factory, s.reqHandler, s.skillch); err != nil {
			log.Printf("EmbeddedServer.RunOnce() : Error Encountered From Server : %s", err.Error())
		}
	} else {
		log.Printf("EmbeddedServer.RunOnce(): Server has been terminated explicitly. Terminate.")
	}
}

/////////////////////////////////////////////////////////////////////////////
// QuorumVerifier
/////////////////////////////////////////////////////////////////////////////

func (s *EmbeddedServer) HasQuorum(count int) bool {
	return count == 1
}

/////////////////////////////////////////////////////////////////////////////
// ServerCallback Interface
/////////////////////////////////////////////////////////////////////////////

//
// Callback when a new proposal arrives
//
func (s *EmbeddedServer) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {

	fid := proposal.GetFid()
	reqId := proposal.GetReqId()
	txnid := proposal.GetTxnid()
	opCode := common.OpCode(proposal.GetOpCode())

	// If this host is the one that sends the request to the leader
	if fid == s.handler.GetFollowerId() {
		s.state.mutex.Lock()
		defer s.state.mutex.Unlock()

		// look up the request handle from the pending list and
		// move it to the proposed list
		handle, ok := s.state.pendings[reqId]
		if ok {
			delete(s.state.pendings, reqId)

			if opCode == common.OPCODE_ABORT || opCode == common.OPCODE_RESPONSE {
				handle.CondVar.L.Lock()
				defer handle.CondVar.L.Unlock()
				
				if len(proposal.GetKey()) != 0 {
					handle.Err = errors.New(proposal.GetKey())
				}
				
				handle.CondVar.Signal()
			} else {
				s.state.proposals[common.Txnid(txnid)] = handle
			}
		}
	}
}

//
// Callback when a commit arrives
//
func (s *EmbeddedServer) UpdateStateOnCommit(txnid common.Txnid, key string) {

	log.Printf("EmbeddedServer.UpdateStateOnCommit(): Committing proposal %d key %s.", txnid, key)

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and
	// notify the waiting goroutine that the request is done.
	handle, ok := s.state.proposals[txnid]

	if ok {
		log.Printf("EmbeddedServer.UpdateStateOnCommit(): Notify client for proposal %d", txnid)

		delete(s.state.proposals, txnid)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		handle.CondVar.Signal()
	}
}

func (s *EmbeddedServer) GetStatus() protocol.PeerStatus {
	return s.state.getStatus()
}

func (s *EmbeddedServer) UpdateWinningEpoch(epoch uint32) {
	// any new tnxid from now on will use the new epoch
	s.txn.SetEpoch(epoch)
}

func (s *EmbeddedServer) GetEnsembleSize() uint64 {
	return 1
}

func (s *EmbeddedServer) GetFollowerId() string {
	return s.msgAddr
}
