package server 

import (
	"log"
	"net"
	rpc "net/rpc"
	"sync"
	"common"
	http "net/http"
	"time"
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

type Request struct {
	OpCode	string
	Key		string
	Value	[]byte
}

type Reply struct {
	Result	[]byte
}

var gHandler *RequestReceiver = nil

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Start a new RequestListener for listening to new client request. 
// laddr - local network address (host:port)
//
func StartRequestListener(laddr string, server *Server) (*RequestListener, error) {

	if gHandler == nil {
		// first time initializatino
		gHandler = &RequestReceiver{server : server} 
		rpc.Register(gHandler)
		rpc.HandleHTTP()
	} else {
		gHandler.setServer(server)
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
// Set the server
//
func (s *RequestReceiver) setServer(server *Server) {
	s.server = server
}

//
// Handle a new incoming request
//
//func (s *RequestReceiver) NewRequest(message []byte, reply *[]byte) error {
func (s *RequestReceiver) NewRequest(req *Request, reply **Reply) error {

	if s.server.IsDone() {
		return common.NewError(common.SERVER_ERROR, "Server is terminated. Cannot process new request.")
	}

	log.Printf("RequestReceiver.NewRequest(): Receive request from client")
	log.Printf("RequestReceiver.NewRequest(): opCode %s key %s value %s", req.OpCode, req.Key, req.Value)
		
	opCode := common.GetOpCode(req.OpCode)
	if opCode == common.OPCODE_GET {
	
		result, err := s.server.GetValue(req.Key)
		if err != nil {
			return err
		}
		log.Printf("RequestReceiver.NewRequest(): Receive response from server, len(value) = %d", len(result)) 
	
		*reply = &Reply{Result : result}
		return nil
	
	} else 	if opCode == common.OPCODE_ADD || 
			opCode == common.OPCODE_SET || 
			opCode == common.OPCODE_DELETE {
			
		if req.Value == nil {
			req.Value = ([]byte)("")		
		}
			
		id := uint64(time.Now().UnixNano())
		request := s.server.factory.CreateRequest(id,
			uint32(common.GetOpCode(req.OpCode)),	
			req.Key,
			req.Value)
		
		handle := newRequestHandle(request)

		handle.condVar.L.Lock()
		defer handle.condVar.L.Unlock()

		// push the request to a channel
		log.Printf("Handing new request to server. Key %s", req.Key)
		s.server.state.incomings <- handle

		// This goroutine will wait until the request has been processed.
		handle.condVar.Wait()
		log.Printf("Receive Response for request. Key %s", req.Key)

		*reply = &Reply{Result : nil}
		return handle.err
		
	} else {
		return common.NewError(common.CLIENT_ERROR, fmt.Sprintf("Invalid Op code %s", req.OpCode))
	}
}