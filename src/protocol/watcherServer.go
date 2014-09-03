 package protocol 

import (
	"github.com/jliang00/gometa/src/common"
	"net"
	"log"
	"fmt"
	"time"
	"runtime/debug"
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
func RunWatcherServer(host string,
					peerUDP []string,
					peerTCP []string,
					handler ActionHandler,
					factory MsgFactory,
					killch <- chan bool) {

	backoff := common.RETRY_BACKOFF
	retry := true 
	for retry {					
		peer, err := findPeerToConnect(host, peerUDP, peerTCP, factory,  handler, killch) 
		if err == nil { 
			if peer != "" {
				if err = runOnce(host, peer, handler, factory, killch); err == nil {
					// if runOnce() terminates without an error, it is killed.  So return.
					retry = false
				} else {
					log.Printf("WatcherServer.RunWatcherServer() : got error = %s.  Retry.", err)
				}
			} else {
				// if there is no peer found, return.  
				retry = false
			}
		} else {
			log.Printf("WatcherServer.RunWatcherServer() : got error = %s.  Retry.", err)
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
	
func runOnce(naddr string,
			peer string,
			handler ActionHandler,
			factory MsgFactory,
			killch <- chan bool) (err error) {

	// Catch panic at the main entry point for WatcherServer
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in RunWatcherServer() : %s\n", r)
			log.Printf("RunWatcherServer() terminates : Diagnostic Stack ...")
			log.Printf("%s", debug.Stack())	
			
			err = r.(error)
		}
	}()

	// create connection with a peer 
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		return err
	}
	pipe := common.NewPeerPipe(conn)
	log.Printf("WatcherServer.RunWatcherServer() : Watcher %s successfully created TCP connection to peer %s", naddr, peer)

	// close the connection to the peer. If connection is closed,
	// sync proxy and watcher will also terminate by err-ing out.  
	// If sync proxy and watcher terminates the pipe upon termination, 
	// it is ok to close it again here.
	defer common.SafeRun("WatcherServer.runWatcherServer()",
		func() {
			pipe.Close()
		})

	// start syncrhorniziing with the metadata server 
	success, isKilled := syncWithPeer(naddr, pipe, handler, factory, killch)

	// run watcher after synchronization
	if success {
		isKilled = runWatcher(pipe, handler, factory, killch)
		log.Printf("WatcherServer.RunWatcherServer() : Watcher Server %s terminate", naddr)
		
		if !isKilled {
			err = common.NewError(common.SERVER_ERROR, fmt.Sprintf("Watcher %s terminated unexpectedly.", naddr))
			return err
		}
		return nil
		
	} else if !isKilled {
		err = common.NewError(common.SERVER_ERROR, fmt.Sprintf("Watcher %s fail to synchronized with peer %s", 
				naddr, peer))
		return err
	} else {
		return nil
	}
}

/////////////////////////////////////////////////////////////////////////////
// WatcherServer - Private Function
/////////////////////////////////////////////////////////////////////////////

//
// Synchronize with the leader.  
//
func syncWithPeer(naddr string,
    pipe *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory,
	killch <- chan bool) (bool, bool) {
	
	log.Printf("WatcherServer.syncWithPeer(): Watcher %s start synchronization with peer (TCP %s)", 
			naddr, pipe.GetAddr())
	proxy := NewFollowerSyncProxy(pipe, handler, factory, false)
	donech := proxy.GetDoneChannel()
	go proxy.Start()
	defer proxy.Terminate() 

	// This will block until NewWatcherSyncProxy has sychronized with the peer (a bool is pushed to donech)
	select {
	case success := <-donech:
		if success {
			log.Printf("WatcherServer.syncWithPeer(): Watcher %s done synchronization with peer (TCP %s)", 
					naddr, pipe.GetAddr())
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
// Run Watcher Protocol
//
func runWatcher(pipe *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory,
	killch <- chan bool) bool {

	// Create a watcher.  The watcher will start a go-rountine, listening to messages coming from peer.
	log.Printf("WatcherServer.runWatcher(): Start Watcher Protocol")
	watcher := NewFollower(WATCHER, pipe, handler, factory)
	donech := watcher.Start()
	defer watcher.Terminate()
	
	//start main processing loop
	return runTillEnd(killch, donech)
}

//
// main processing loop
//
func runTillEnd(
	killch <-chan bool,
	donech <-chan bool) bool {
	
	log.Printf("WatcherServer.runTillEnd()")

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

//
// Find which peer to connect to
//
func findPeerToConnect(host    	string,
					  peerUDP 	[]string,
					  peerTCP 	[]string,
					  factory 	MsgFactory,
					  handler 	ActionHandler,
					  killch 	<- chan bool) (string, error) {
					  
	// Run master election to figure out who is the leader.  Only connect to leader for now.
	site, err := CreateElectionSite(host, peerUDP, factory, handler, true)
	if err != nil {
		return "", err
	}
	
	defer func() {
		common.SafeRun("Server.cleanupState()",
			func() {
				site.Close()
			})
	}()

	resultCh := site.StartElection()
	if resultCh == nil {
		return "", common.NewError(common.PROTOCOL_ERROR, "WatcherServer.findPeerToConnect: Election Site is in progress or is closed.") 
	}
		
	select {
		case leader, ok := <-resultCh : 
			if !ok {
				return "", common.NewError(common.PROTOCOL_ERROR, "WatcherServer.findPeerToConnect: Election Fails") 
			}
			
			for i, peer := range peerUDP {
				if peer == leader {
					return peerTCP[i],	nil
				}
			}

			return "", common.NewError(common.PROTOCOL_ERROR, 
						"WatcherServer.findPeerToConnect : Cannot find matching port for peer. Peer UPD port = " + leader) 
					
		case <- killch :
			return "", nil
	}
}