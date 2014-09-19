package protocol

import (
	"github.com/couchbase/gometa/common"
	"log"
	"runtime/debug"
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
func RunWatcherServer(leader string,
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool) {

	var once sync.Once
	backoff := common.RETRY_BACKOFF
	retry := true
	for retry {
		if runOnce(leader, handler, factory, killch, readych, once) {
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

//
// Create a new WatcherServer. This is a blocking call until
// the WatcherServer terminates. Make sure the kilch is a buffered
// channel such that if the goroutine running RunWatcherServer goes
// away, the sender won't get blocked.
//
func RunWatcherServerWithElection(host string,
	peerUDP []string,
	peerTCP []string,
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

		if peer != "" && runOnce(peer, handler, factory, killch, readych, once) {
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
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool,
	once sync.Once) (isKilled bool) {

	// Catch panic at the main entry point for WatcherServer
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in WatcherServer.runOnce() : %s\n", r)
			log.Printf("WatcherServer.runOnce() terminates : Diagnostic Stack ...")
			log.Printf("%s", debug.Stack())
		}
	}()

	// create connection with a peer
	conn, err := createConnection(peer)
	if err != nil {
		log.Printf("WatcherServer.runOnce() error : %s", err)
		return false
	}
	pipe := common.NewPeerPipe(conn)
	log.Printf("WatcherServer.runOnce() : Watcher successfully created TCP connection to peer %s", peer)

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
		if !runWatcher(pipe, handler, factory, killch, readych, once) {
			log.Printf("WatcherServer.runOnce() : Watcher terminated unexpectedly.")
			return false
		}

	} else if !isKilled {
		log.Printf("WatcherServer.runOnce() : Watcher fail to synchronized with peer %s", peer)
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

	log.Printf("WatcherServer.syncWithPeer(): Watcher start synchronization with peer (TCP %s)", pipe.GetAddr())
	proxy := NewFollowerSyncProxy(pipe, handler, factory, false)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate()

	// This will block until NewWatcherSyncProxy has sychronized with the peer (a bool is pushed to donech)
	select {
	case success = <-donech:
		if success {
			log.Printf("WatcherServer.syncWithPeer(): Watcher done synchronization with peer (TCP %s)", pipe.GetAddr())
		}
		return success, false
	case <-killch:
		// simply return. The pipe will eventually be closed and
		// cause WatcherSyncProxy to err out.
		log.Printf("WatcherServer.syncWithPeer(): Recieve kill singal.  Synchronization with peer (TCP %s) terminated.",
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
			log.Printf("panic in findPeerToConnect() : %s\n", r)
			log.Printf("findPeerToConnect() terminates : Diagnostic Stack ...")
			log.Printf("%s", debug.Stack())
		}
	}()

	// Run master election to figure out who is the leader.  Only connect to leader for now.
	site, err := CreateElectionSite(host, peerUDP, factory, handler, true)
	if err != nil {
		log.Printf("WatcherServer.findPeerToConnect() error : %s", err)
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
		log.Printf("WatcherServer.findPeerToConnect: Election Site is in progress or is closed.")
		return "", false
	}

	select {
	case leader, ok := <-resultCh:
		if !ok {
			log.Printf("WatcherServer.findPeerToConnect: Election Fails")
			return "", false
		}

		for i, peer := range peerUDP {
			if peer == leader {
				return peerTCP[i], false
			}
		}

		log.Printf("WatcherServer.findPeerToConnect : Cannot find matching port for peer. Peer UPD port = %s", leader)
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
	handler ActionHandler,
	factory MsgFactory,
	killch <-chan bool,
	readych chan<- bool,
	once sync.Once) (isKilled bool) {

	// Create a watcher.  The watcher will start a go-rountine, listening to messages coming from peer.
	log.Printf("WatcherServer.runWatcher(): Start Watcher Protocol")
	watcher := NewFollower(WATCHER, pipe, handler, factory)
	donech := watcher.Start()
	defer watcher.Terminate()

	// notify that the watcher is starting to run.  Only do this once.
	once.Do(func() { readych <- true })

	select {
	case <-killch:
		// server is being explicitly terminated.  Terminate the watcher go-rountine as well.
		log.Printf("WatcherServer.runTillEnd(): receive kill signal. Terminate.")
		return true
	case <-donech:
		// watcher is done.  Just return.
		log.Printf("WatcherServer.runTillEnd(): Watcher go-routine terminates. Terminate.")
		return false
	}
}
