package server 

import (
	"log"
	"net"
	rpc "net/rpc"
	"sync"
	"common"
	http "net/http"
	"protocol"
	"fmt"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type RequestListener struct {
	naddr    	string
	listener 	net.Listener
	
	mutex    	sync.Mutex
	isClosed 	bool	
}

type RequestReceiver struct {
	server	*Server
}

var gHasRegistered bool = false

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Start a new RequestListener for listening to new client request. 
// laddr - local network address (host:port)
//
func StartRequestListener(laddr string, server *Server) (*RequestListener, error) {

	if !gHasRegistered {
		handler := &RequestReceiver{server : server} 
		rpc.Register(handler)
		rpc.HandleHTTP()
		gHasRegistered = true
	}

	li, err := net.Listen(common.MESSAGE_TRANSPORT_TYPE, laddr)
	if err != nil {
		return nil, err
	}
	go http.Serve(li, nil)
	
	listener := &RequestListener{naddr: laddr, listener : li}
	return listener, nil
}

//
// Close the listener.  This does not reclaim the exisiting client conection
// immediately, but it will stop new connection.  
//
func (li *RequestListener) Close() {
	li.mutex.Lock()
	li.mutex.Unlock()
	
	if !li.isClosed {
		li.isClosed = true
		li.listener.Close()	
	}
}

//
// Handle a new incoming request
//
func (s *RequestReceiver) NewRequest(message []byte, reply *[]byte) error {

	if s.server.IsDone() {
		return common.NewError(common.SERVER_ERROR, "Server is terminated. Cannot process new request.")
	}
	
	req, err := s.doMarshall(message) 	
	if err != nil { 
		log.Printf("RequestReceiver.doMarshall() : ecounter error when unmarshalling mesasage from client.")
		log.Printf("Error = %s. Ingore client request.", err.Error())	
		*reply = nil 
		return err 
	}

	// TODO : Assign an unique id to the request msg
	id := uint64(1)
	request := s.server.factory.CreateRequest(id,
		req.GetOpCode(),
		req.GetKey(),
		req.GetContent())

	handle := newRequestHandle(request)

	handle.condVar.L.Lock()
	defer handle.condVar.L.Unlock()

	// push the request to a channel
	log.Printf("Handing new request to server. Key %s", req.GetKey())
	s.server.state.incomings <- handle

	// This goroutine will wait until the request has been processed.
	handle.condVar.Wait()
	log.Printf("Receive Response for request. Key %s", req.GetKey())

	*reply = nil 
	return handle.err
}

//
// Marshall client request
//
func (s *RequestReceiver) doMarshall(msg []byte) (protocol.RequestMsg, error) {

	// skip the total length (first 8 bytes)
	packet, err := common.UnMarshall(msg[8:])
	if err != nil {
		return nil, err
	}
	
	switch request := packet.(type) {
	case protocol.RequestMsg:
		log.Printf("RequestReceiver.doMarshall() : Message decoded.  Packet = %s", request.Name())
		request.Print()
		return request, nil
	default:
		return nil, common.NewError(common.CLIENT_ERROR, 
			fmt.Sprintf("Cannot process client reqeust of message type %s.", request.Name()))
	}
}
