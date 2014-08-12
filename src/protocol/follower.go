package protocol

import (
	"common"
	"log"
	"sync"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type Follower struct {
	kind     PeerRole
	pipe     *common.PeerPipe
	pendings []ProposalMsg
	handler  ActionHandler
	factory  MsgFactory
	
	mutex    sync.Mutex
	isClosed bool
	donech   chan bool
	killch   chan bool
}

/////////////////////////////////////////////////
// Follower - Public Function
/////////////////////////////////////////////////

//
// Create a new Follower.  This will run the
// follower protocol to communicate with the leader
// in voting proposal as well as sending new proposal
// to leader.   
//
func NewFollower(kind PeerRole,
	pipe *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory) *Follower {

	follower := &Follower{kind: kind,
		pipe:     pipe,
		pendings: make([]ProposalMsg, 0, common.MAX_PROPOSALS),
		handler:  handler,
		factory:  factory,
		isClosed: false,
		donech :  make(chan bool, 1), // make buffered channel so sender won't block	
		killch :  make(chan bool, 1)} // make buffered channel so sender won't block	


	return follower
}

//
// Start the listener.  This is running in a goroutine.
// The follower can be shutdown by calling Terminate() 
// function or by closing the PeerPipe.
//
func (f *Follower) Start() (<- chan bool) {

	go f.startListener()
	
	return f.donech
}

//
// Return the follower ID
//
func (f *Follower) GetId() string {
	return f.pipe.GetAddr()
}

//
// Forward the request to the leader
//
func (f *Follower) ForwardRequest(request RequestMsg) bool {
	return f.pipe.Send(request)
}

//
// Terminate.  This function is an no-op if the
// follower already complete successfully. 
//
func (f *Follower) Terminate() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if !f.isClosed {	
		f.isClosed = true
		f.killch <- true
	}
}

/////////////////////////////////////////////////
// Follower - Private Function
/////////////////////////////////////////////////

//
// Goroutine.  Start a new listener for the follower.
// Listen to any new message coming from the leader.
// This is the main routine for the follower to interact
// with the leader.  If there is any error (network
// error commuincating to leader or internal failure),
// this loop will terminate.   The server (that contains
// the follower) will need to run leader election again.
//
func (f *Follower) startListener() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in Follower.startListener() : %s\n", r)
		}
		
	    common.SafeRun("Follower.startListener()",
			func() {
				f.close()  
				f.donech <- true
			})
	}()
	
	reqch := f.pipe.ReceiveChannel()

	for {
		select {
			case msg, ok := <-reqch :
				if ok {
					err := f.handleMessage(msg.(common.Packet))
					if err != nil {
						// If there is an error, terminate
						log.Printf("Follower.startListener(): There is an error in handling leader message.  Error = %s.  Terminate.",
							err.Error())
						return
					}
				} else {
					log.Printf("Follower.startListener(): message channel closed.  Terminate.")
					return
				}
			case <- f.killch :
				return
		}
	}
}

//
// Signal that the follower is closed (done)
//
func (f *Follower) close() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.isClosed = true	
}

//
// Handle message from the leader.
//
func (f *Follower) handleMessage(msg common.Packet) (err error) {
	switch request := msg.(type) {
	case ProposalMsg:
		err = f.handleProposal(request)
	case CommitMsg:
		err = f.handleCommit(request)
	default:
		log.Printf("Follower.handleMessage(): unrecognized message %s.  Ignore.", msg.Name())
	}
	return err
}

//
// Handle proposal message from the leader.
//
func (f *Follower) handleProposal(msg ProposalMsg) error {
	// TODO : Check if the txnid is the next one (last txnid + 1)
	// ZK will only warn

	// TODO: call service to log the proposal

	// Add to pending list
	f.pendings = append(f.pendings, msg)

	// Send Accept Message
	// TODO: Should only accept if the txnid exceeeds the last one.
	//       ZK does not do that.  Need to double check Paxos Protocol.
	return f.sendAccept(common.Txnid(msg.GetTxnid()), f.GetId())
}

//
// Handle commit message from the leader.
//
func (f *Follower) handleCommit(msg CommitMsg) error {

	// If there is nothing pending, ignore this commit.
	if len(f.pendings) == 0 {
		return nil
	}

	// check if the commit is the first one in the pending list.
	// If not, ZK will crash the follower.    All commits are
	// processed sequentially.
	next := f.pendings[0]
	if next == nil || next.GetTxnid() != msg.GetTxnid() {
		// TODO : Log an error and panic
	}

	// remove proposal from pendings
	p := f.pendings[0]
	f.pendings = f.pendings[1:]

	// commit
	err := f.handler.Commit(p)
	if err != nil {
		log.Println("Follower.Commit(): Error in commit.  Error = %s", err.Error())	
		// TODO : throw error
	}

	// TODO: send response to client
	// TODO: update election site
	return nil
}

//
// Send accept message to the leader.
//
func (f *Follower) sendAccept(txnid common.Txnid, fid string) error {
	accept := f.factory.CreateAccept(uint64(txnid), fid)

	// Send the message to the leader through a reliable protocol (TCP).
	success := f.pipe.Send(accept)
	if !success {
		// It is a fatal error if not able to send to the leader.  It will require the server to
		// do leader election again.
		return common.NewError(common.FATAL_ERROR, "Fail to send accept message for to leader from "+fid)
	}

	return nil
}
