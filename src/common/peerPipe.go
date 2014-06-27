package common 

import (
    "encoding/binary"
	"net"
	"sync"
	"log"
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
	conn          net.Conn
	sendch        chan Packet
	receivech     chan Packet
	mutex         sync.Mutex
	isClosed      bool
}

/////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////

// 
// Create a new PeerPipe. The consumer can call
// ReceiveChannel() to get the channel for receving
// message packets.  
//
func NewPeerPipe(pconn net.Conn) (*PeerPipe, error) {

	pipe := &PeerPipe{conn : pconn,
					  sendch : make(chan Packet, MAX_PROPOSALS*2),
					  receivech : make(chan Packet, MAX_PROPOSALS*2),
					  isClosed : false}

	go pipe.doSend() 					  
	go pipe.doReceive() 					  
	return pipe, nil
}

//
// Get the net address of the remote peer.
//
func (p *PeerPipe) GetAddr() string {
	return p.conn.RemoteAddr().String()
}

//
// Return the receive channel.  If PeerPipe is closed, then
// return nil.
//
func (p *PeerPipe) ReceiveChannel() (<-chan Packet) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.isClosed {
		return (<-chan Packet)(p.receivech)
	}
	return nil
}

//
// Close the PeerPipe.  It is safe to call this
// method multiple times without causing panic.
//
func (p *PeerPipe) Close() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.isClosed {
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
			log.Printf("panic in PeerPipe.doSend() : %s\n", r)
		}
		
		// This will close the Send and Receive channel 
		p.Close()
	}()

	for {
		packet, ok := <- p.sendch 
		if !ok {
			// channel close.  Terminate the loop.
			break	
		} 
		
		msg, err := Marshall(packet) 
		if err != nil {
			break
		}
		size := len(msg)
		
		// write the packet
		n, err := p.conn.Write(msg)
		if n < size || err != nil {
			// Network error. Close the loop.
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
			log.Printf("panic in PeerPipe.doReceive() : %s\n", r)
		}
		
		// This will close the Send and Receive channel 
		p.Close()
	}()

	for {
		// read the size of the packet (uint64)
		var lenBuf []byte = make([]byte, 8)
		n, err := p.conn.Read(lenBuf)
		if n < len(lenBuf) || err != nil {
			// if encountering an error, kill the pipe.
			return
		}
	
		// read the content	
        size := binary.BigEndian.Uint64(lenBuf)		
        buf := make([]byte, size)
		n, err = p.conn.Read(buf)
		if uint64(n) < size || err != nil {
			// if encountering an error, kill the pipe.
			return
		}
	
		// unmarshall the content and put it in the channel
		packet, err := UnMarshall(buf)
		if err != nil {
			break
		}
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