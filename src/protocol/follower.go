package protocol 

import (
	"common"
)

/////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////

type Follower struct {
	kind		 PeerRole	
	pipe         *common.PeerPipe
	pendings     []ProposalMsg
	handler      ActionHandler
	factory      MsgFactory
}

/////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////

//
// Create a new Follower.
//
func NewFollower(kind PeerRole, pipe *common.PeerPipe, handler ActionHandler, factory MsgFactory) *Follower {
	follower := &Follower{kind : kind,
	                      pipe: pipe, 
	                      pendings : make([]ProposalMsg, common.MAX_PROPOSALS),
	                      handler : handler,
	                      factory : factory} 
	                      
	go follower.startListener()
	
	return follower
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
func (f *Follower) ForwardRequest(request RequestMsg) {
	f.pipe.Send(request)
}

/////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////

//
// Goroutine.  Start a new listener for the follower.
// Listen to any new message coming from the leader.
//
func (f* Follower) startListener() {
    reqch := f.pipe.ReceiveChannel()

loop:
    for {
        select {
        case msg, ok := <-reqch:
            if ok {
                // TODO : handle error from handleMessage() 
               	err := f.handleMessage(msg.(common.Packet)) 
               	if err != nil {
               	}
            } else {
        		// TODO : the channel is closed.  Need to shutdown the server itself.
                break loop
            }
        }
    }
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
    	// TODO : if we don't recoginize the message.  Just log it and ignore.
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
	f.sendAccept(common.Txnid(msg.GetTxnid()), f.GetId())
	
	return nil
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
		// TODO : throw error
	}
	
	// TODO: send response to client 	
	// TODO: update election site
	return nil
}

//
// Send accept message to the leader.  
//
func (f *Follower) sendAccept(txnid common.Txnid, fid string ) {
	accept := f.factory.CreateAccept(uint64(txnid), fid)
	
	f.pipe.Send(accept)
}
