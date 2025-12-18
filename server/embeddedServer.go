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
	"sync"
	"time"

	"github.com/couchbase/gometa/action"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	r "github.com/couchbase/gometa/repository"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type EmbeddedServer struct {
	msgAddr     string
	quota       uint64
	repo        r.IRepository
	log         r.CommitLogger
	srvConfig   *r.ServerConfig
	txn         *common.TxnState
	state       *ServerState
	factory     protocol.MsgFactory
	handler     *action.ServerAction
	notifier    action.EventNotifier
	reqHandler  protocol.CustomRequestHandler
	listener    *common.PeerListener
	skillch     chan bool
	initialized bool
	mutex       sync.RWMutex
	resetCh     chan bool
	authfn      common.ServerAuthFunction
	params      r.RepoFactoryParams
}

/////////////////////////////////////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////////////////////////////////////

func RunEmbeddedServer(msgAddr string) (*EmbeddedServer, error) {

	return RunEmbeddedServerWithNotifier(msgAddr, nil)
}

func RunEmbeddedServerWithNotifier(msgAddr string, notifier action.EventNotifier) (*EmbeddedServer, error) {
	return RunEmbeddedServerWithCustomHandler(
		msgAddr,
		notifier,
		nil,
		common.FDB_REPOSITORY_NAME,
		uint64(0),
	)
}

func RunEmbeddedServerWithCustomHandler(msgAddr string,
	notifier action.EventNotifier,
	reqHandler protocol.CustomRequestHandler,
	repoName string,
	memory_quota uint64) (*EmbeddedServer, error) {

	return RunEmbeddedServerWithCustomHandler2(msgAddr, notifier, reqHandler,
		repoName, memory_quota, uint64(60*15), uint8(0), uint64(0))
}

func RunEmbeddedServerWithCustomHandler2(msgAddr string,
	notifier action.EventNotifier,
	reqHandler protocol.CustomRequestHandler,
	repoName string,
	memory_quota uint64,
	sleepDur uint64,
	threshold uint8,
	minFileSize uint64) (*EmbeddedServer, error) {

	return RunEmbeddedServerWithCustomHandler3(msgAddr, notifier, reqHandler,
		repoName, memory_quota, sleepDur, threshold, minFileSize, nil)
}

func RunEmbeddedServerWithCustomHandler3(
	msgAddr string,
	notifier action.EventNotifier,
	reqHandler protocol.CustomRequestHandler,
	repoBaseDir string,
	memory_quota uint64,
	sleepDur uint64,
	threshold uint8,
	minFileSize uint64,
	authfn common.ServerAuthFunction,
) (*EmbeddedServer, error) {
	return RunEmbeddedServerWithCustomHandler4(
		msgAddr,
		notifier,
		reqHandler,
		authfn,
		r.RepoFactoryParams{
			Dir:                        repoBaseDir,
			MemoryQuota:                memory_quota,
			CompactionTimerDur:         sleepDur,
			CompactionMinFileSize:      minFileSize,
			CompactionThresholdPercent: threshold,
			StoreType:                  r.FDbStoreType,
			EnableWAL:                  false,
		},
	)
}

func RunEmbeddedServerWithCustomHandler4(
	msgAddr string,
	notifier action.EventNotifier,
	reqHandler protocol.CustomRequestHandler,
	authfn common.ServerAuthFunction,
	openParams r.RepoFactoryParams,
) (*EmbeddedServer, error) {

	server := new(EmbeddedServer)
	server.msgAddr = msgAddr
	server.notifier = notifier
	server.reqHandler = reqHandler
	server.params = openParams
	server.authfn = authfn

	if err := server.bootstrap(); err != nil {
		log.Current.Errorf("EmbeddedServer.boostrap: error : %v\n", err)
		return nil, err
	}

	go server.run()

	return server, nil
}

// Terminate the Server
func (s *EmbeddedServer) Terminate() {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	if s.state.done {
		return
	}

	s.state.done = true

	s.skillch <- true // kill leader/follower server
}

// Reset Connections
func (s *EmbeddedServer) ResetConnections() error {

	// restart listener
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return nil
	}

	// reset listener
	listener, err := s.listener.ResetConnections()
	if err != nil {
		return err
	}
	s.listener = listener

	time.Sleep(100 * time.Millisecond)

	// reset leaderServer and leader
	s.resetCh <- true

	return nil
}

// Check if server is terminated
func (s *EmbeddedServer) IsDone() bool {

	s.state.mutex.RLock()
	defer s.state.mutex.RUnlock()

	return s.state.done
}

// Check if server is active
func (s *EmbeddedServer) IsActive() bool {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.isActiveNoLock()
}

// Check if server is active
func (s *EmbeddedServer) isActiveNoLock() bool {

	s.state.mutex.RLock()
	defer s.state.mutex.RUnlock()

	return s.initialized && !s.state.done
}

// Retrieve value
func (s *EmbeddedServer) GetValue(key string) ([]byte, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return nil, common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.handler.Get(key)
}

// Set value
func (s *EmbeddedServer) SetValue(key string, value []byte) {
	s.Set(key, value)
}

// Delete value
func (s *EmbeddedServer) DeleteValue(key string) {
	s.Delete(key)
}

func (s *EmbeddedServer) postToIncomings(handle *protocol.RequestHandle) error {

	s.mutex.RLock()
	if !s.isActiveNoLock() {
		s.mutex.RUnlock()
		return common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	state := s.state
	s.mutex.RUnlock()

	// We do not want to hold s.mutex when pushing request handle to channel.  But this means that
	// there is a slight chance that s.state may been re-initailzied in bootstrap().  In this case,
	// the variable 'state' is pointing to a stale state object that has just being replaced in bootstrap().
	// Without holding lock, let's proceed to place the request object into the channel optimistically.
	state.incomings <- handle

	// Now that we successfully place the request in channel.  Check if the state object has
	// become stale.  If it is stale, the request may still get processed, since we don't know
	// when it gets stale.  In this case, return error assuming that the request will not get processed.
	state.mutex.RLock()
	defer state.mutex.RUnlock()
	if state.done {
		return common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination.  Request may still get processed.")
	}

	return nil
}

// Set value
func (s *EmbeddedServer) Set(key string, value []byte) error {

	id, err := common.NewUUID()
	if err != nil {
		return err
	}

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_SET),
		key,
		value)

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Current.Tracef("EmbeddedServer.Set(): Handing new request to gometa leader. Key %s", key)
	if err := s.postToIncomings(handle); err != nil {
		return err
	}

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Current.Tracef("EmbeddedServer.Set(): Receive Response from gometa leader. Key %s", key)

	return handle.Err
}

// Broadcast value
func (s *EmbeddedServer) Broadcast(key string, value []byte) error {

	id, err := common.NewUUID()
	if err != nil {
		return err
	}

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_BROADCAST),
		key,
		value)

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Current.Tracef("EmbeddedServer.Broadcast(): Handing new request to gometa leader. Key %s", key)
	if err := s.postToIncomings(handle); err != nil {
		return err
	}

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Current.Tracef("EmbeddedServer.Broadcast(): Receive Response from gometa leader. Key %s", key)

	return handle.Err
}

// Set value
func (s *EmbeddedServer) MakeRequest(op common.OpCode, key string, value []byte) error {

	id, err := common.NewUUID()
	if err != nil {
		return err
	}

	request := s.factory.CreateRequest(id,
		uint32(op),
		key,
		value)

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Current.Tracef("EmbeddedServer.MakeRequest(): Handing new request to gometa leader. Key %s", key)
	if err := s.postToIncomings(handle); err != nil {
		return err
	}

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Current.Tracef("EmbeddedServer.MakeRequest(): Receive Response from gometa leader. Key %s", key)

	return handle.Err
}

func (s *EmbeddedServer) MakeAsyncRequest(op common.OpCode, key string, value []byte) error {

	id, err := common.NewUUID()
	if err != nil {
		return err
	}

	request := s.factory.CreateRequest(id,
		uint32(op),
		key,
		value)

	handle := newRequestHandle(request)

	// push the request to a channel
	log.Current.Tracef("EmbeddedServer.MakeAsyncRequest(): Handing new request to gometa leader. Key %s", key)
	if err := s.postToIncomings(handle); err != nil {
		return err
	}

	return nil
}

// Delete value
func (s *EmbeddedServer) Delete(key string) error {

	id, err := common.NewUUID()
	if err != nil {
		return err
	}

	request := s.factory.CreateRequest(id,
		uint32(common.OPCODE_DELETE),
		key,
		[]byte(""))

	handle := newRequestHandle(request)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	// push the request to a channel
	log.Current.Tracef("Handing new request to server. Key %s", key)
	if err := s.postToIncomings(handle); err != nil {
		return err
	}

	// This goroutine will wait until the request has been processed.
	handle.CondVar.Wait()
	log.Current.Tracef("Receive Response for request. Key %s", key)

	return handle.Err
}

// Create a new iterator
func (s *EmbeddedServer) GetIterator(startKey, endKey string) (r.IRepoIterator, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return nil, common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.repo.NewIterator(r.MAIN, startKey, endKey)
}

func (s *EmbeddedServer) GetServerConfigIterator(startKey, endKey string) (r.IRepoIterator, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return nil, common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.repo.NewIterator(r.SERVER_CONFIG, startKey, endKey)
}

func (s *EmbeddedServer) SetConfigValue(key string, value string) error {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.srvConfig.LogStr(key, value)
}

func (s *EmbeddedServer) DeleteConfigValue(key string) error {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.srvConfig.Delete(key)
}

func (s *EmbeddedServer) GetConfigValue(key string) (string, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.isActiveNoLock() {
		return "", common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")
	}

	return s.srvConfig.GetStr(key)
}

/////////////////////////////////////////////////////////////////////////////
// Server
/////////////////////////////////////////////////////////////////////////////

// change server state
func (s *EmbeddedServer) changeServerState() {

	newState := newServerState()
	if s.state != nil {
		newState.done = s.state.done

		//mark the old state as done and cleanup
		s.state.mutex.Lock()
		defer s.state.mutex.Unlock()
		s.state.done = true
		cleanupServerState(s.state)
	}

	s.state = newState
}

// Bootstrp
func (s *EmbeddedServer) bootstrap() (err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.initialized = false

	defer func() {
		r := recover()
		if r != nil {
			log.Current.Errorf("panic in EmbeddedServer.bootstrap() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
		}

		if err != nil || r != nil {
			common.SafeRun("EmbeddedServer.bootstrap()",
				func() {
					s.cleanup()
				})
		}
	}()

	// cleanup existing state
	s.cleanup()

	// Initialize server state
	s.changeServerState()

	// Create and initialize new txn state.
	s.txn = common.NewTxnState()

	// Initialize repository service
	s.repo, err = r.OpenOrCreateNewRepositoryFromParams(s.params)
	if err != nil {
		return err
	}

	// Initialize server config
	s.srvConfig = r.NewServerConfig(s.repo)

	// initialize the current transaction id to the lastLoggedTxid.  This
	// is the txid that this node has seen so far.  If this node becomes
	// the leader, a new epoch will be used and new current txid will
	// be generated. So no need to initialize the epoch at this point.
	lastLoggedTxid, err := s.srvConfig.GetLastLoggedTxnId()
	if err != nil {
		return err
	}
	s.txn.InitCurrentTxnid(common.Txnid(lastLoggedTxid))

	// Initialize commit log
	lastCommittedTxid, err := s.srvConfig.GetLastCommittedTxnId()
	if err != nil {
		return err
	}
	s.log, err = r.NewTransientCommitLog(s.repo, lastCommittedTxid)
	if err != nil {
		return err
	}

	// Initialize various callback facility for leader election and
	// voting protocol.
	s.factory = message.NewConcreteMsgFactory()
	s.handler = action.NewServerActionWithNotifier(s.repo, s.log, s.srvConfig, s, s.notifier, s.txn, s.factory, s)
	s.skillch = make(chan bool, 1) // make it buffered to unblock sender
	s.resetCh = make(chan bool, 1) // make it buffered to unblock sender

	// Need to start the peer listener before election. A follower may
	// finish its election before a leader finishes its election. Therefore,
	// a follower node can request a connection to the leader node before that
	// node knows it is a leader.  By starting the listener now, it allows the
	// follower to establish the connection and let the leader handles this
	// connection at a later time (when it is ready to be a leader).
	s.listener, err = common.StartPeerListener2(s.msgAddr, s.authfn)
	if err != nil {
		err = common.WrapError(common.SERVER_ERROR, "Fail to start PeerListener. err = %v", err)
		return
	}

	s.initialized = true

	return nil
}

func (s *EmbeddedServer) run() {

	defer func() {
		common.SafeRun("EmbeddedServer.run()",
			func() {
				s.mutex.Lock()
				defer s.mutex.Unlock()
				s.cleanup()
			})
	}()

	for {
		s.runOnce()
		if !s.IsDone() {
			time.Sleep(time.Duration(200) * time.Millisecond)

			if !s.IsDone() {
				if err := s.bootstrap(); err != nil {
					log.Current.Errorf("EmbeddedServer.boostrap: error : %v\n", err)
				}
			}
		} else {
			break
		}
	}
}

// Cleanup internal state upon exit
func (s *EmbeddedServer) cleanup() {

	common.SafeRun("EmbeddedServer.cleanup()",
		func() {
			if s.listener != nil {
				s.listener.Close()
				s.listener = nil
			}
		})

	common.SafeRun("EmbeddedServer.cleanup()",
		func() {
			if s.repo != nil {
				s.repo.Close()
				s.repo = nil
			}
		})

	cleanupServerState(s.state)
}

func cleanupServerState(state *ServerState) {

	if state == nil {
		return
	}

	for len(state.incomings) > 0 {
		request := <-state.incomings
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanup()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range state.pendings {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanup()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range state.proposals {
		request.Err = common.NewError(common.SERVER_ERROR, "Terminate Request due to server termination")

		common.SafeRun("EmbeddedServer.cleanup()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}
}

// Run the server until it stop.  Will not attempt to re-run.
func (s *EmbeddedServer) runOnce() {

	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in EmbeddedServer.runOnce() : %v\n", r)
			log.Current.Errorf("Diagnostic Stack ...")
			log.Current.Errorf("%s", log.Current.StackTrace())
		}
	}()

	log.Current.Infof("EmbeddedServer.runOnce() : Start Running Server")

	// Check if the server has been terminated explicitly. If so, don't run.
	if s.IsActive() {

		// runServer() is done if there is an error	or being terminated explicitly (killch)
		s.state.setStatus(protocol.LEADING)
		if err := protocol.RunLeaderServerWithCustomHandler(
			s.msgAddr, s.listener, s.state, s.handler, s.factory, s.reqHandler, s.skillch, s.resetCh); err != nil {
			log.Current.Errorf("EmbeddedServer.RunOnce() : Error Encountered From Server : %s", err.Error())
		}
	} else {
		log.Current.Infof("EmbeddedServer.RunOnce(): Server has been terminated explicitly. Terminate.")
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

// Callback when a new proposal arrives
func (s *EmbeddedServer) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {

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

func (s *EmbeddedServer) UpdateStateOnRespond(fid string, reqId uint64, err string, content []byte) {

	// If this host is the one that sends the request to the leader
	if fid == s.handler.GetFollowerId() {
		s.state.mutex.Lock()

		// look up the request handle from the pending list and
		// move it to the proposed list
		handle, ok := s.state.pendings[reqId]
		if ok {
			delete(s.state.pendings, reqId)
			s.state.mutex.Unlock()

			handle.CondVar.L.Lock()
			defer handle.CondVar.L.Unlock()

			if len(err) != 0 {
				handle.Err = errors.New(err)
			}

			handle.CondVar.Signal()
		} else {
			s.state.mutex.Unlock()
		}
	}
}

// Callback when a commit arrives
func (s *EmbeddedServer) UpdateStateOnCommit(txnid common.Txnid, key string) {

	log.Current.Debugf("EmbeddedServer.UpdateStateOnCommit(): Committing proposal %d key %s.", txnid, key)

	s.state.mutex.Lock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and
	// notify the waiting goroutine that the request is done.
	handle, ok := s.state.proposals[txnid]

	if ok {
		log.Current.Debugf("EmbeddedServer.UpdateStateOnCommit(): Notify client for proposal %d", txnid)

		delete(s.state.proposals, txnid)
		s.state.mutex.Unlock()

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		handle.CondVar.Signal()
	} else {
		s.state.mutex.Unlock()
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
