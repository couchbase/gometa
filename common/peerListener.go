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

package common

import (
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/security"
	"net"
	"sync"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type ServerAuthFunction func(conn net.Conn) (*PeerPipe, Packet, error)

//
// PeerListener - Listener for TCP connection
// If there is a new connection request, send
// the new connection through connch.  If
// the listener is closed, connch will be closed.
//
type PeerListener struct {
	naddr     string
	listener  net.Listener
	connch    chan *PeerConn
	mutex     sync.Mutex
	isClosed  bool
	resetConn bool
	donech    chan bool
	authfn    ServerAuthFunction
}

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Start a new PeerListener for listening to new connection request for
// processing messages.
// laddr - local network address (host:port)
//
func StartPeerListener(laddr string) (*PeerListener, error) {

	return startPeerListener(laddr, make(chan *PeerConn, MAX_PEERS), nil)
}

func StartPeerListener2(laddr string, authfn ServerAuthFunction) (*PeerListener, error) {

	return startPeerListener(laddr, make(chan *PeerConn, MAX_PEERS), authfn)
}

func startPeerListener(laddr string, connch chan *PeerConn, authfn ServerAuthFunction) (*PeerListener, error) {

	listener := &PeerListener{
		naddr:    laddr,
		connch:   connch,
		isClosed: false,
		donech:   make(chan bool),
		authfn:   authfn,
	}

	li, err := security.MakeListener(laddr)
	if err != nil {
		return nil, err
	}
	listener.listener = li

	go listener.listen()

	return listener, nil
}

//
// Get the channel for new peer connection request.
// Return nil if the listener is closed. The consumer
// should also check if the channel is closed when
// dequeueing from the channel.
//
func (l *PeerListener) ConnChannel() <-chan *PeerConn {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.isClosed {
		return (<-chan *PeerConn)(l.connch)
	}
	return nil
}

//
// Close the PeerListener.  The connection channel will be closed
// as well.  This function is syncronized and will not close the
// channel twice.
//
func (l *PeerListener) Close() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.CloseNoLock()
}

func (l *PeerListener) CloseNoLock() bool {
	if !l.isClosed {
		l.isClosed = true

		log.Current.Debugf("PeerListener.Close(): local address %s", l.naddr)
		log.Current.Debugf("%s", "PeerListener.Close() : Diagnostic Stack ...")
		log.Current.LazyDebug(log.Current.StackTrace)

		SafeRun("PeerListener.Close()",
			func() {
				if !l.resetConn {
					close(l.connch)
				}
			})
		SafeRun("PeerListener.Close()",
			func() {
				l.listener.Close()
			})

		close(l.donech)

		return true
	}

	return false
}

//
// IMP: ResetConnections algorithm will trigger closing of the existing
// PeerListener and starting of a new one. But it uses the same connch
// so that the leaderServer shouldn't need a restart.
//
func (l *PeerListener) ResetConnections() (*PeerListener, error) {

	l.mutex.Lock()
	l.resetConn = true
	l.mutex.Unlock()

	SafeRun("PeerListener.Close()",
		func() {
			l.listener.Close()
		})

	<-l.donech

	// At this point, the PeerListener is closed as the l.donech is closed.
	// So, there shouldn't be any net.Listener listening on l.laddr and hence
	// it is safe to call startPeerListener(l.naddr, l.connch)

EMPTY:
	for {
		select {
		case <-l.connch:
		default:
			break EMPTY
		}
	}

	return startPeerListener(l.naddr, l.connch, l.authfn)
}

/////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////

//
// Goroutine.  This function listens to any incoming new network
// request.  It will send the new connection onto the connection
// channel (connch).   If there is an error or the listener is
// closed, this function will terminate, as well as closing the
// connection channel (if it is not yet closed).
//
func (l *PeerListener) listen() {

	selfRestart := func() {
		l.mutex.Lock()
		defer l.mutex.Unlock()

		if l.isClosed {
			return
		}

		// If the connections were reset, close this PeerListener object and
		// let ResetConnections create a new PeerListener.
		if l.resetConn {
			l.CloseNoLock()
			return
		}

		SafeRun("PeerListener.listen.selfRestart()",
			func() {
				l.listener.Close()
			})

		li, err := security.MakeListener(l.naddr)
		if err != nil {
			log.Current.Errorf("PeerListener.listen() error in selfRestart:MakeListener %v", err)
			panic(err)
		}
		l.listener = li

		go l.listen()
	}

	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerListener.listen() : %s\n", r)
		}

		selfRestart()
	}()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// if there is error, just terminate the listener loop.
			log.Current.Errorf("PeerListener.listen(): Error in accepting new connection.  Error = %s. Terminate.", err.Error())
			break
		}

		go l.handleConnection(conn)
	}
}

func (l *PeerListener) handleConnection(conn net.Conn) {

	var pipe *PeerPipe
	var packet Packet
	var err error

	var peerConn *PeerConn

	if l.authfn != nil {
		if pipe, packet, err = l.authfn(conn); err != nil {
			log.Current.Errorf("PeerListener.handleConnection error in authfn %v for conn %v:%v",
				err, conn.LocalAddr().String(), conn.RemoteAddr().String())
			conn.Close()
			return
		}
	}

	peerConn = NewPeerConn(conn, pipe, packet)

	// We get a new connection.  Pass it to the channel and let
	// the consumer handle the connection.
	l.queue(peerConn)
}

//
// Put the connection onto the channel for consumption.
// This function acquires the mutex to ensure that the
// there is no goroutine concurrently try to close
// the listener.
//
func (l *PeerListener) queue(conn *PeerConn) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.isClosed {
		l.connch <- conn
	}
}
