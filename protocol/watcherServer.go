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
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// WatcherServer - Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new WatcherServer. This is a blocking call until
// the WatcherServer terminates. Make sure the kilch is a buffered
// channel such that if the goroutine running RunWatcherServer goes
// away, the sender won't get blocked.
//
func RunWatcherServerWithRequest(leader string,
	requestMgr RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool,
	alivech chan<- bool,
	pingch <-chan bool) {

	var once sync.Once
	backoff := common.RETRY_BACKOFF
	retry := true
	for retry {
		log.Current.Debugf("WatcherServer.runWatcherServer() : runOnce() returns with error.  Retry ...")
		if runOnce(leader, requestMgr, handler, factory, killch, readych, alivech, pingch, &once) {
			retry = false
		}

		if retry {
			timer := time.NewTimer(backoff * time.Millisecond)
			select {
			case <-timer.C:
			case <-killch:
				return
			}

			backoff += backoff
			if backoff > common.MAX_RETRY_BACKOFF {
				backoff = common.MAX_RETRY_BACKOFF
			}
		}
	}
}

//
// Create a new WatcherServer. This is a blocking call until
// the WatcherServer terminates. Make sure the kilch is a buffered
// channel such that if the goroutine running RunWatcherServer goes
// away, the sender won't get blocked.
//
func RunWatcherServer(leader string,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool) {

	RunWatcherServerWithRequest(leader, nil, handler, factory, killch, readych, make(chan bool, 1), make(chan bool, 1))
}

//
// Create a new WatcherServer. This is a blocking call until
// the WatcherServer terminates. Make sure the kilch is a buffered
// channel such that if the goroutine running RunWatcherServer goes
// away, the sender won't get blocked.
//
func RunWatcherServerWithElection(host string,
	peerUDP []string,
	peerTCP []string,
	requestMgr RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool) {

	var once sync.Once
	backoff := common.RETRY_BACKOFF
	retry := true
	for retry {
		peer, isKilled := findPeerToConnect(host, peerUDP, peerTCP, factory, handler, killch)
		if isKilled {
			return
		}

		if peer != "" && runOnce(peer, requestMgr, handler, factory, killch, readych, make(chan bool, 1), make(chan bool, 1), &once) {
			retry = false
		}

		if retry {
			timer := time.NewTimer(backoff * time.Millisecond)
			<-timer.C

			backoff += backoff
			if backoff > common.MAX_RETRY_BACKOFF {
				backoff = common.MAX_RETRY_BACKOFF
			}
		}
	}
}

/////////////////////////////////////////////////////////////////////////////
// WatcherServer - Execution Loop
/////////////////////////////////////////////////////////////////////////////

func runOnce(peer string,
	requestMgr RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool,
	alivech chan<- bool,
	pingch <-chan bool,
	once *sync.Once) (isKilled bool) {

	// Catch panic at the main entry point for WatcherServer
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in WatcherServer.runOnce() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
		} else {
			log.Current.Debugf("WatcherServer.runOnce() terminates.")
			log.Current.Tracef(log.Current.StackTrace())
		}

		if requestMgr != nil {
			requestMgr.CleanupOnError()
		}
	}()

	// create connection with a peer
	conn, err := createConnection(peer)
	if err != nil {
		log.Current.Errorf("WatcherServer.runOnce() error : %s", err)
		return false
	}
	pipe := common.NewPeerPipe(conn)
	log.Current.Debugf("WatcherServer.runOnce() : Watcher successfully created TCP connection to peer %s", peer)

	// close the connection to the peer. If connection is closed,
	// sync proxy and watcher will also terminate by err-ing out.
	// If sync proxy and watcher terminates the pipe upon termination,
	// it is ok to close it again here.
	defer common.SafeRun("WatcherServer.runOnce()",
		func() {
			pipe.Close()
		})

	// start syncrhorniziing with the metadata server
	success, isKilled := syncWithPeer(pipe, handler, factory, killch)

	// run watcher after synchronization
	if success {
		if !runWatcher(pipe, requestMgr, handler, factory, killch, readych, alivech, pingch, once) {
			log.Current.Errorf("WatcherServer.runOnce() : Watcher terminated unexpectedly.")
			return false
		}

	} else if !isKilled {
		log.Current.Errorf("WatcherServer.runOnce() : Watcher fail to synchronized with peer %s", peer)
		return false
	}

	return true
}

/////////////////////////////////////////////////////////////////////////////
// WatcherServer - Election and Synchronization
/////////////////////////////////////////////////////////////////////////////

//
// Synchronize with the leader.
//
func syncWithPeer(pipe *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool) (success bool, isKilled bool) {

	log.Current.Infof("WatcherServer.syncWithPeer(): Watcher start synchronization with peer (TCP %s)", pipe.GetAddr())
	proxy := NewFollowerSyncProxy(pipe, handler, factory, false)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate()

	// This will block until NewWatcherSyncProxy has sychronized with the peer (a bool is pushed to donech)
	select {
	case success = <-donech:
		if success {
			log.Current.Infof("WatcherServer.syncWithPeer(): Watcher done synchronization with peer (TCP %s)", pipe.GetAddr())
		}
		return success, false
	case <-killch:
		// simply return. The pipe will eventually be closed and
		// cause WatcherSyncProxy to err out.
		log.Current.Infof("WatcherServer.syncWithPeer(): Recieve kill singal.  Synchronization with peer (TCP %s) terminated.",
			pipe.GetAddr())
		return false, true
	}
}

//
// Find which peer to connect to
//
func findPeerToConnect(host string,
	peerUDP []string,
	peerTCP []string,
	factory MsgFactory,
	handler ActionHandler,
	killch <-chan bool) (leader string, isKilled bool) {

	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in findPeerToConnect() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
		} else {
			log.Current.Debugf("findPeerToConnect() terminates : Diagnostic Stack ...")
			log.Current.LazyDebug(log.Current.StackTrace)
		}
	}()

	// Run master election to figure out who is the leader.  Only connect to leader for now.
	site, err := CreateElectionSite(host, peerUDP, factory, handler, true)
	if err != nil {
		log.Current.Errorf("WatcherServer.findPeerToConnect() error : %s", err)
		return "", false
	}

	defer func() {
		common.SafeRun("Server.cleanupState()",
			func() {
				site.Close()
			})
	}()

	resultCh := site.StartElection()
	if resultCh == nil {
		log.Current.Errorf("WatcherServer.findPeerToConnect: Election Site is in progress or is closed.")
		return "", false
	}

	select {
	case leader, ok := <-resultCh:
		if !ok {
			log.Current.Errorf("WatcherServer.findPeerToConnect: Election Fails")
			return "", false
		}

		for i, peer := range peerUDP {
			if peer == leader {
				return peerTCP[i], false
			}
		}

		log.Current.Errorf("WatcherServer.findPeerToConnect : Cannot find matching port for peer. Peer UPD port = %s", leader)
		return "", false

	case <-killch:
		return "", true
	}
}

/////////////////////////////////////////////////////////////////////////////
// WatcherServer - Watcher Protocol
/////////////////////////////////////////////////////////////////////////////

//
// Run Watcher Protocol
//
func runWatcher(pipe *common.PeerPipe,
	requestMgr RequestMgr,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool,
	alivech chan<- bool,
	pingch <-chan bool,
	once *sync.Once) (isKilled bool) {

	// Create a watcher.  The watcher will start a go-rountine, listening to messages coming from peer.
	log.Current.Infof("WatcherServer.runWatcher(): Start Watcher Protocol")
	watcher := NewFollower(WATCHER, pipe, handler, factory)
	donech := watcher.Start()
	defer watcher.Terminate()

	// notify that the watcher is starting to run.  Only do this once.
	once.Do(func() { readych <- true })

	log.Current.Infof("WatcherServer.runWatcher(): Watcher is ready to process request")

	var incomings <-chan *RequestHandle
	if requestMgr != nil {
		incomings = requestMgr.GetRequestChannel()
	} else {
		incomings = make(chan *RequestHandle)
	}

	for {
		select {
		case handle, ok := <-incomings:
			if ok {
				// move request to pending queue (waiting for proposal)
				requestMgr.AddPendingRequest(handle)

				// forward the request to the leader
				if !watcher.ForwardRequest(handle.Request) {
					log.Current.Errorf("WatcherServer.processRequest(): fail to send client request to leader. Terminate.")
					return
				}
			} else {
				log.Current.Debugf("WatcherServer.processRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-killch:
			// server is being explicitly terminated.  Terminate the watcher go-rountine as well.
			log.Current.Debugf("WatcherServer.runTillEnd(): receive kill signal. Terminate.")
			return true
		case <-donech:
			// watcher is done.  Just return.
			log.Current.Debugf("WatcherServer.runTillEnd(): Watcher go-routine terminates. Terminate.")
			return false
		case <-pingch:
			if len(alivech) == 0 {
				alivech <- true
			}
		}
	}
}
