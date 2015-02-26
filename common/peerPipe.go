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
	"bytes"
	"encoding/binary"
	"github.com/couchbase/gometa/log"
	"net"
	"sync"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

//
// PeerPipe is to maintain the messaging channel
// between two peers. This is not used for leader
// election.
//
type PeerPipe struct {
	conn      net.Conn
	sendch    chan Packet
	receivech chan Packet
	mutex     sync.Mutex
	isClosed  bool
}

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Create a new PeerPipe. The consumer can call
// ReceiveChannel() to get the channel for receving
// message packets.
//
func NewPeerPipe(pconn net.Conn) *PeerPipe {

	pipe := &PeerPipe{conn: pconn,
		sendch:    make(chan Packet, MAX_PROPOSALS*2),
		receivech: make(chan Packet, MAX_PROPOSALS*2),
		isClosed:  false}

	go pipe.doSend()
	go pipe.doReceive()
	return pipe
}

//
// Get the net address of the remote peer.
//
func (p *PeerPipe) GetAddr() string {
	return p.conn.RemoteAddr().String()
}

//
// Return the receive channel.
//
func (p *PeerPipe) ReceiveChannel() <-chan Packet {

	// Just return receivech even if it is closed.  The caller
	// can tell if the channel is closed by using multi-value
	// recieve operator.  Returning a nil channel can cause
	// the caller being block forever.
	//
	return (<-chan Packet)(p.receivech)
}

//
// Close the PeerPipe.  It is safe to call this
// method multiple times without causing panic.
//
func (p *PeerPipe) Close() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {

		log.Current.Debugf("PeerPipe.Close(): Remote Address %s", p.GetAddr())
		log.Current.Debugf("%s", "PeerPipe.Close() : Diagnostic Stack ...")
		log.Current.LazyDebug(log.Current.StackTrace)

		p.isClosed = true

		SafeRun("PeerPipe.Close()",
			func() {
				p.conn.Close()
			})
		SafeRun("PeerPipe.Close()",
			func() {
				close(p.sendch)
			})
		SafeRun("PeerPipe.Close()",
			func() {
				close(p.receivech)
			})
		return true
	}

	return false
}

//
// Send a packet to the peer. This method will return
// false if the pipe is already closed.
//
func (p *PeerPipe) Send(packet Packet) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		p.sendch <- packet
		return true
	}
	return false
}

/////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////

//
// Goroutine.  Go through the send channel and
// send out each packet to the peer as bytes.
//
func (p *PeerPipe) doSend() {
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerPipe.doSend() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
		}

		// This will close the Send and Receive channel
		p.Close()
	}()

	for {
		packet, ok := <-p.sendch
		if !ok {
			// channel close.  Terminate the loop.
			log.Current.Infof("%s", "PeerPipe.doSend() : Send channel closed.  Terminate.")
			return
		}

		log.Current.Debugf("PeerPipe.doSend() : Prepare to send message %s to Peer %s", packet.Name(), p.GetAddr())
		log.Current.LazyDebug(packet.String)

		msg, err := Marshall(packet)
		if err != nil {
			log.Current.Errorf("PeerPipe.doSend() : Fail to marshall message %s to Peer %s. Terminate.", packet.Name(), p.GetAddr())
			return
		}
		size := len(msg)

		// write the packet
		log.Current.Debugf("PeerPipe.doSend() : Sending message %s (len %d) to Peer %s", packet.Name(), size, p.GetAddr())
		n, err := p.conn.Write(msg)
		if n < size || err != nil {
			// Network error. Close the loop.  The pipe will
			// close and cause subsequent Send() to fail.
			log.Current.Debugf("PeerPipe.doSend() : ecounter error when sending mesasage to Peer %s.  Error = %s.  Terminate.",
				p.GetAddr(), err.Error())
			return
		}
	}
}

//
// Goroutine.  Listen to the connection and
// unmarshall each packet.  Forward the packet to
// receive channel.
func (p *PeerPipe) doReceive() {
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerPipe.doReceive() : %s\n", r)
			log.Current.Errorf("%s", log.Current.StackTrace())
		}

		// This will close the Send and Receive channel
		p.Close()
	}()

	for {
		// read packet len
		lenBuf, err := p.readBytes(8)
		if err != nil {
			// if encountering an error, kill the pipe.
			log.Current.Debugf("PeerPipe.doRecieve() : ecounter error when received mesasage from Peer.  Error = %s. Kill Pipe.",
				err.Error())
			return
		}

		// read the content
		size := binary.BigEndian.Uint64(lenBuf)
		buf, err := p.readBytes(size)
		if err != nil {
			// if encountering an error, kill the pipe.
			log.Current.Debugf("PeerPipe.doRecieve() : ecounter error when received mesasage from Peer.  Error = %s. Kill Pipe.",
				err.Error())
			return
		}
		// unmarshall the content and put it in the channel
		packet, err := UnMarshall(buf)
		if err != nil {
			log.Current.Errorf("PeerPipe.doRecieve() : ecounter error when unmarshalling mesasage from Peer.  Error = %s. Terminate.",
				err.Error())
			return
		}
		log.Current.Debugf("PeerPipe.doRecieve() : Message decoded.  Packet = %s", packet.Name())
		log.Current.LazyDebug(packet.String)

		// This can block if the reciever of the channel is slow or terminated premauturely (which cause channel to fill up).
		// In this case, this can cause the TCP connection to fail.  The other end of the pipe will close as a result
		// of this.  This end of the pipe will eventually close since it can no longer send message to the other end.
		p.queue(packet)
	}
}

//
// Queue the packe to the recieve channel if
// the channel has not been closed.
//
func (p *PeerPipe) queue(packet Packet) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		p.receivech <- packet
	}
}

func (p *PeerPipe) readBytes(len uint64) ([]byte, error) {

	result := new(bytes.Buffer)
	remaining := len

	for {
		// read the size of the packet (uint64)
		buf := make([]byte, remaining)
		n, err := p.conn.Read(buf)
		log.Current.Debugf("PeerPipe.readBytes() : Receiving message from Peer %s, bytes read %d", p.GetAddr(), n)

		if n != 0 {
			result.Write(buf[0:n])
			remaining = remaining - uint64(n)

			if remaining == 0 {
				return result.Bytes(), nil
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return result.Bytes(), nil
}
