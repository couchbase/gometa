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
	"net"
	"sync"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

//
// PeerListener - Listener for TCP connection
// If there is a new connection request, send
// the new connection through connch.  If
// the listener is closed, connch will be closed.
//
type PeerListener struct {
	naddr    string
	listener net.Listener
	connch   chan net.Conn
	mutex    sync.Mutex
	isClosed bool
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

	listener := &PeerListener{naddr: laddr,
		connch:   make(chan net.Conn, MAX_PEERS),
		isClosed: false}

	li, err := net.Listen(MESSAGE_TRANSPORT_TYPE, laddr)
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
func (l *PeerListener) ConnChannel() <-chan net.Conn {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.isClosed {
		return (<-chan net.Conn)(l.connch)
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

	if !l.isClosed {
		l.isClosed = true

		log.Current.Debugf("PeerListener.Close(): local address %s", l.naddr)
		log.Current.Debugf("%s", "PeerListener.Close() : Diagnostic Stack ...")
		log.Current.LazyDebug(log.Current.StackTrace)

		SafeRun("PeerListener.Close()",
			func() {
				close(l.connch)
			})
		SafeRun("PeerListener.Close()",
			func() {
				l.listener.Close()
			})
		return true
	}

	return false
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
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerListener.listen() : %s\n", r)
		}

		// This will close the connection channel
		l.Close()
	}()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// if there is error, just terminate the listener loop.
			log.Current.Errorf("PeerListener.listen(): Error in accepting new connection.  Error = %s. Terminate.", err.Error())
			break
		}

		// We get a new connection.  Pass it to the channel and let
		// the consumer handle the connection.
		l.queue(conn)
	}
}

//
// Put the connection onto the channel for consumption.
// This function acquires the mutex to ensure that the
// there is no goroutine concurrently try to close
// the listener.
//
func (l *PeerListener) queue(conn net.Conn) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.isClosed {
		l.connch <- conn
	}
}
