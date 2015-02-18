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
// PeerMessenger sends packets between peers.
//
type PeerMessenger struct {
	conn      net.PacketConn
	sendch    chan *Message
	receivech chan *Message
	splitter  map[string]chan *Message
	mutex     sync.Mutex
	isClosed  bool
}

// A wrapper of a UPD message (content + sender addr)
type Message struct {
	Content Packet
	Peer    net.Addr
}

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Create a new PeerMessenger. The consumer can call
// ReceiveChannel() to get the channel for receving
// message packets.   If the splitter is not specified,
// then the message will be sent out through the channel
// returned by ReceiveChannel().  Otherwise, the splitter
// map will be used to decide which channel to use for
// incoming message.  The key to the map is name of the
// Packet (Packet.Name()).  If the splitter does not
// map to a channel for the given name, then it will
// send the message out through the default channel.
//
// If the messenger is closed, the splitter channels will
// be closed as well.
//
func NewPeerMessenger(laddr string, splitter map[string]chan *Message) (*PeerMessenger, error) {

	pconn, err := getConn(laddr)
	if err != nil {
		return nil, err
	}

	pipe := &PeerMessenger{conn: pconn,
		sendch:    make(chan *Message, MAX_PROPOSALS*2),
		receivech: make(chan *Message, MAX_PROPOSALS*2),
		splitter:  splitter,
		isClosed:  false}

	go pipe.doSend()
	go pipe.doReceive()
	return pipe, nil
}

//
// Return the default receive channel.  If a splitter is specified, then
// it will first use the channel in the splitter map.  If the splitter is
// not specified or the splitter map does not map to a channel, then
// the default receive channel is used.
//
func (p *PeerMessenger) DefaultReceiveChannel() <-chan *Message {

	// Just return receivech even if it is closed.  The caller
	// can tell if the channel is closed by using multi-value
	// recieve operator.  Returning a nil channel can cause
	// the caller being block forever.
	//
	return (<-chan *Message)(p.receivech)
}

//
// Get the receiving channel for the specific message name. If there is
// no match, the return the default receiving channel.
//
func (p *PeerMessenger) ReceiveChannel(msgName string) <-chan *Message {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	ch, ok := p.splitter[msgName]
	if ok {
		return (<-chan *Message)(ch)
	}
	return (<-chan *Message)(p.receivech)
}

//
// Get the local net address.
//
func (p *PeerMessenger) GetLocalAddr() string {
	return p.conn.LocalAddr().String()
}

//
// Close the PeerMessenger.  It is safe to call this
// method multiple times without causing panic.
//
func (p *PeerMessenger) Close() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {

		log.Current.Debugf("PeerMessenger.Close() : Local Addr %s", p.GetLocalAddr())
		log.Current.Debugf("%s", "PeerMessenger.Close() : Diagnostic Stack ...")
		log.Current.LazyDebug(log.Current.StackTrace)

		p.isClosed = true

		SafeRun("PeerMessenger.Close()",
			func() {
				p.conn.Close()
			})
		SafeRun("PeerMessenger.Close()",
			func() {
				close(p.sendch)
			})
		SafeRun("PeerMessenger.Close()",
			func() {
				close(p.receivech)
			})

		if p.splitter != nil {
			for _, ch := range p.splitter {
				SafeRun("PeerMessenger.Close()",
					func() {
						close(ch)
					})
			}
		}

		return true
	}

	return false
}

//
// Send a packet to the peer. This method will return
// false if the pipe is already closed.
//
func (p *PeerMessenger) Send(packet Packet, peer net.Addr) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		p.sendch <- &Message{packet, peer}
		return true
	}
	return false
}

//
// Send a packet to the peer. This method will return
// false if the pipe is already closed or there is error
// in resolving the peer addr.
//
func (p *PeerMessenger) SendByName(packet Packet, peer string) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		addr, err := net.ResolveUDPAddr("udp", peer)
		if err == nil {
			p.sendch <- &Message{packet, addr}
			return true
		}
	}
	return false
}

//
// Send a packet to the all the peers. This method will return
// false if the pipe is already closed.
//
func (p *PeerMessenger) Multicast(packet Packet, peers []net.Addr) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		for i := range peers {
			p.sendch <- &Message{packet, peers[i]}
		}
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
func (p *PeerMessenger) doSend() {
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerMessenger.doSend() : %s\n", r)
		}

		// This will close the Send and Receive channel
		p.Close()
	}()

	for {
		msg, ok := <-p.sendch
		if !ok {
			// channel close.  Terminate the loop.
			log.Current.Infof("PeerMessenger.doSend() : Send channel closed.  Terminate.")
			break
		}

		log.Current.Debugf("PeerMessenger.doSend() : Preparing message %s to Peer %s", msg.Content.Name(), msg.Peer.String())
		log.Current.LazyDebug(msg.Content.String)

		serialized, err := Marshall(msg.Content)
		if err != nil {
			log.Current.Infof("PeerMessenger.doSend() : Fail to marshall message to Peer %s", msg.Peer.String())
			continue
		}
		size := len(serialized)

		// write the packet
		log.Current.Debugf("PeerMessenger.doSend() : Sending message %s (len %d) to Peer %s", msg.Content.Name(), size, msg.Peer.String())
		n, err := p.conn.WriteTo(serialized, msg.Peer)
		if n < size || err != nil {
			log.Current.Debugf("PeerMessenger.doSend() : ecounter error when sending mesasage to Peer %s.  Error = %s",
				msg.Peer.String(), err.Error())
		}
	}
}

//
// Goroutine.  Listen to the connection and
// unmarshall each packet.  Forward the packet to
// receive channel.
func (p *PeerMessenger) doReceive() {
	defer func() {
		if r := recover(); r != nil {
			log.Current.Errorf("panic in PeerMessenger.doReceive() : %s\n", r)
		}

		// This will close the Send and Receive channel
		p.Close()
	}()

	for {
		// read the size of the packet (uint64)
		//var lenBuf []byte = make([]byte, 8)
		buf := make([]byte, MAX_DATAGRAM_SIZE)
		n, peer, err := p.conn.ReadFrom(buf)
		if err != nil {
			log.Current.Errorf(
				"PeerMessenger.doRecieve() : ecounter error when received mesasage from Peer.  Error = %s. Terminate.",
				err.Error())
			return
		}
		log.Current.Debugf("PeerMessenger.doRecieve() : Receiving message from Peer %s, bytes read %d", peer.String(), n)

		// unmarshall the content and put it in the channel
		// skip the first 8 bytes (total len)
		packet, err := UnMarshall(buf[8:n])
		if err != nil {
			log.Current.Errorf(
				"PeerMessenger.doRecieve() : ecounter error when unmarshalling mesasage from Peer.  Error = %s. Terminate.",
				err.Error())
			break
		}
		log.Current.Debugf("PeerMessenger.doRecieve() : Message decoded.  Packet = %s", packet.Name())
		log.Current.LazyDebug(packet.String)

		// This can block if the reciever of the channel is slow or terminated premauturely (which cause channel to filled up).
		// In this case, this can cause the UDP connection to fail such that other nodes will no longer send message to this node.
		p.queue(&Message{Content: packet, Peer: peer})
	}
}

//
// Queue the packe to the recieve channel if
// the channel has not been closed.
//
func (p *PeerMessenger) queue(message *Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isClosed {
		ch := p.receivech
		name := message.Content.Name()
		if p.splitter != nil && p.splitter[name] != nil {
			ch = p.splitter[name]
		}

		ch <- message
	}
}

//
// Get a connection to receive packet
//
func getConn(laddr string) (net.PacketConn, error) {

	addrObj, err := net.ResolveUDPAddr(ELECTION_TRANSPORT_TYPE, laddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP(ELECTION_TRANSPORT_TYPE, addrObj)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
