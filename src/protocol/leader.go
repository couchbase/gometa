package protocol

import (
	"github.com/jliang00/gometa/src/common"
	"sync"
	"log"
	"fmt"
	"runtime/debug"
)

/////////////////////////////////////////////////
// Proposal Processing for ZK
/////////////////////////////////////////////////

// 1) TCP is used as the network protocol (guarantee packet ordering)
// 2) The leader sends the proposal sequentially based on the order of
//    received requests.  Due to (1), it expects the proposal to be
//    arrived at each follower in the same order.
// 3) Each follower processes the proposal in the same sequential order
//    Therefore, the ack/accept is returned in the same order as the proposal.
// 4) In the follower, proposal and commit can be processed out-of-order.
//    These 2 messages go through different queues on the follower side.
// 5) The leader has a dedicated go-routine (2 threads in ZK) to send/recieve 
//    proposal/commit for a specific follower.  If messaging err-out,  
//    it will close the socket.  This, in turn, will terminate both the sending 
//    and recieving threads and force the go-routine to shutdown.  
//    In  doing so, the leader will also remove the follower.  The leader will 
//    listen to any new socket connection to re-estabilish communication with the follower.
// 6) When the follower fails to send a Ack/Accept to the leader, it will close the socket.
//    This, in turn, will shutdown the follower.  The main thread will be back to
//    election (QuorumPeer in "looking" state).
// 7) When the follower re-connect to the leader, the leader will go through the following:
//    a) open the commit log and re-send {proposals, committed messages} to the follower.
//       The leader will hold the read lock on the commit log as to avoid any concurrent
//       commit being written to the log.
//    b) new pending {proposal, commit} will be sent to follower.
//    c) new pending proposal will also be sent (note it is synchornized such that
//        no pending proposal is added during this step).
//    d) add follower as the participant of future proposal.
// 8) Due to (7), the proposal must be committed serially in order.  The proposal cannot
//    be skipped. The voting can stall if the leader lose the majority (by design).
//

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

type Leader struct {
	naddr         	string
	handler       	ActionHandler
	factory       	MsgFactory
	
	notifications   chan *notification
	lastCommitted 	common.Txnid
	quorums       	map[common.Txnid][]string
	proposals       map[common.Txnid]ProposalMsg
		
	// mutex protected variable
	mutex     		sync.Mutex
	followers 		map[string]*messageListener
	watchers 		map[string]*common.PeerPipe
	observers       map[string]*observer
	isClosed  		bool
	changech        chan bool  	// notify membership of active followers have changed
}

type messageListener struct {
	fid 	  string
	pipe 	  *common.PeerPipe
	leader    *Leader
	killch    chan bool	
}

type notification struct {
	// follower message
	fid			string
	payload		common.Packet
}

/////////////////////////////////////////////////
// Leader - Public Function
/////////////////////////////////////////////////

//
// Create a new leader
//
func NewLeader(naddr string,
	handler ActionHandler,
	factory MsgFactory) (leader *Leader, err error) {

	leader = &Leader{naddr: naddr,
		followers:     	make(map[string]*messageListener),
		watchers:     	make(map[string]*common.PeerPipe),
		observers:     	make(map[string]*observer),
		quorums:       	make(map[common.Txnid][]string),
		proposals:    	make(map[common.Txnid]ProposalMsg),
		notifications:  make(chan *notification, common.MAX_PROPOSALS),
		handler:       	handler,
		factory:       	factory,
		isClosed:  	   	false,
		changech:      	make(chan bool, common.MAX_PEERS)} // make it buffered so sender won't block

	// This is initialized to the lastCommitted in repository. Subsequent commit() will update this 
	// field to the latest committed txnid. This field is used for ensuring the commit order is preserved.  
	// The leader will commit the proposal in the strict order as it arrives as to preserve serializability.
	leader.lastCommitted, err = handler.GetLastCommittedTxid() 
	if err != nil {
		return nil, err
	}

	// start a listener go-routine.  This will be closed when the leader terminate.
	go leader.listen()
	
	return leader, nil
}

//
// Terminate the leader. It is an no-op if the leader is already
// completed successfully.
//
func (l *Leader) Terminate() {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	if !l.isClosed {
		l.isClosed = true
		for _, listener := range l.followers {
			listener.terminate()
		}
	    common.SafeRun("Leader.Terminate()",
			func() {
				close(l.notifications)
			})
	}
}

//
// Has the leader terminated/closed?
//
func (l *Leader) IsClosed() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	return l.isClosed
}

//
// Get the channel for notify when the ensemble of followers
// changes.  The receiver of the channel can then tell 
// if the leader has a quorum of followers.
//
func (l *Leader) GetEnsembleChangeChannel() <-chan bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	return l.changech
}

//
// Get the current ensmeble size of the leader.
// It is the number of followers + 1 (including leader)
//
func (l *Leader) GetActiveEnsembleSize() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	return len(l.followers) + 1
}

//
// Add a watcher. If the leader is terminated, the pipe between leader
// and watcher will also be closed.
//
func (l *Leader) AddWatcher(fid string, 
							peer *common.PeerPipe,
							o *observer) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	// AddWatcher requires holding the mutex such that the leader thread
	// will not be sending new proposal or commit (see sendProposal() and
	// sendCommit()) to watchers.  This allow this function to copy the
	// proposals and commits from the observer queue into the pipe, before
	// the leader has a chance to send new messages.
    for packet := o.getNext(); packet != nil; packet = o.getNext() {		
   
    	switch request := packet.(type) {
			case ProposalMsg:
				txid := common.Txnid(request.GetTxnid())
				log.Printf("Leader.AddWatcher() : send observer's packet %s, txid %d", packet.Name(), txid) 
			case CommitMsg:
				txid := common.Txnid(request.GetTxnid())
				log.Printf("Leader.AddWatcher() : send observer's packet %s, txid %d", packet.Name(), txid) 
		}
		
		peer.Send(packet)    	
    }
    
    // Rememeber the old message listener and start a new one.
	oldPipe , ok := l.watchers[fid]
	l.watchers[fid] = peer 

	// kill the old PeerPipe 
	if ok && oldPipe != nil {
		log.Printf("Leader.AddWatcher() : old PeerPipe found for watcher %s.  Closing old PeerPipe.", fid)
		oldPipe.Close()
	}
}

//
// Add a follower and starts a listener for the follower.
// If the leader is terminated, the pipe between leader
// and follower will also be closed.
//
func (l *Leader) AddFollower(fid string, 
							peer *common.PeerPipe,
							o *observer) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
		
	// AddFollower requires holding the mutex such that the leader thread
	// will not be sending new proposal or commit (see sendProposal() and
	// sendCommit()) to followers.  This allow this function to copy the
	// proposals and commits from the observer queue into the pipe, before
	// the leader has a chance to send new messages.
    for packet := o.getNext(); packet != nil; packet = o.getNext() {		
   
    	switch request := packet.(type) {
			case ProposalMsg:
				txid := common.Txnid(request.GetTxnid())
				log.Printf("Leader.AddFollower() : send observer's packet %s, txid %d", packet.Name(), txid) 
			case CommitMsg:
				txid := common.Txnid(request.GetTxnid())
				log.Printf("Leader.AddFollower() : send observer's packet %s, txid %d", packet.Name(), txid) 
		}
		
		peer.Send(packet)    	
    }
    
    // Rememeber the old message listener and start a new one.
	oldListener, ok := l.followers[fid]
	
	listener := newListener(fid, peer, l)
	l.followers[fid] = listener 
	go listener.start()

	// kill the old message listener	
	if ok && oldListener != nil {
		log.Printf("Leader.AddFollower() : old Listener found for follower %s.  Terminating old listener", fid)
		oldListener.terminate()
	} else {
		// notify a brand new follower (not just replacing an existing one)
		l.changech <- true 
	}
}

// Return the follower ID.  The leader is a follower to itself.
//
func (l *Leader) GetFollowerId() string {
	return l.handler.GetFollowerId()
}

/////////////////////////////////////////////////////////
// Leader - Public Function : Observer 
/////////////////////////////////////////////////////////

//
// Add observer
//
func (l *Leader) AddObserver(id string, o *observer) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	l.observers[id] = o
}

//
// Remove observer
//
func (l *Leader) RemoveObserver(id string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	delete(l.observers, id)	
}

/////////////////////////////////////////////////
// messageListener
/////////////////////////////////////////////////

// Create a new listener
func newListener(fid string, pipe *common.PeerPipe, leader *Leader) *messageListener {

	return &messageListener{fid : fid,
							pipe : pipe,
							leader : leader,
							killch : make(chan bool, 1)}
} 

//
// Gorountine.  Start listener to listen to message from follower.
// Note that each follower has their own receive queue.  This
// is to ensure if the queue is filled up for a single follower,
// only that the connection of that follower may get affected.
// The listener can be killed by calling terminate() or closing 
// the PeerPipe. 
//
func (l *messageListener) start() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in messageListener.start() : %s\n", r)
		}
		
		log.Printf("leader's messageListener.start() terminates : Diagnostic Stack ...")
		log.Printf("%s", debug.Stack())		
		
	    common.SafeRun("messageListener.start()",
			func() {
				l.leader.removeListener(l)
			})
			
	    common.SafeRun("messageListener.start()",
			func() {
				l.pipe.Close()
			})
	}()

	log.Printf("messageListener.start(): start listening to message from peer %s", l.fid)
	reqch := l.pipe.ReceiveChannel()

	for {
		select {
			case req, ok := <-reqch:
				if ok {
					n := &notification{fid : l.fid, payload : req}
					// TODO:  Let's say send is blocked because l.notifications is full, will it becomes unblock
					// when leader.notifications is unblock.
					l.leader.notifications <- n 
				} else {
					// The channel is closed.  Need to shutdown the listener.
					log.Printf("messageListener.start(): message channel closed. Remove peer %s as follower.", l.fid) 
					return
				}
			case <- l.killch:
				log.Printf("messageListener.start(): Listener for %s receive kill signal. Terminate.", l.fid) 
				return
				
		}
	}
}

//
// Terminate the listener.  This should only be called by the leader.
//
func (l *messageListener) terminate() {
	l.killch <- true
}

/////////////////////////////////////////////////////
// Leader - Private Function : Listener Management 
/////////////////////////////////////////////////////

//
// Remove the follower from being tracked
//
func (l *Leader) removeListener(peer *messageListener) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	delete(l.followers, peer.fid)
	
	l.changech <- true 
}

/////////////////////////////////////////////////////////
// Leader - Private Function : Message Processing 
/////////////////////////////////////////////////////////

//
// Main processing message loop for leader.
//
func (l *Leader) listen() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in Leader.listen() : %s\n", r)
		}
		
		log.Printf("Leader.listen() terminates : Diagnostic Stack ...")
		log.Printf("%s", debug.Stack())		
		
	    common.SafeRun("Leader.listen()",
			func() {
				l.Terminate()
			})
	}()

	log.Printf("Leader.listen(): start listening to message for leader")
	
	for {
		select {
			case msg, ok := <- l.notifications:
				if ok {
					if !l.IsClosed() {
						err := l.handleMessage(msg.payload, msg.fid) 
						if err != nil {
							log.Printf("Leader.listen(): Encounter error when processing message %s. Error %s. Terminate", 
								msg.fid, err.Error()) 
							return
						}
					} else {
						log.Printf("Leader.listen(): Leader is closed. Terminate message processing loop.")
						return
					}
				} else {
					// The channel is closed.  
					log.Printf("Leader.listen(): message channel closed. Terminate message processing loop for leader.") 
					return
				}
		}
	}
}

//
// Handle an incoming message based on its type.  All incoming messages from followers are processed serially 
// (by Leader.listen()).  Therefore, the order of corresponding outbound messages (proposal, commit) will be placed 
// in the same order into each follower's pipe.   This implies that we can use 2 state variables (LastLoggedTxid and
// LastCommittedTxid) to determine which peer has the latest repository.  This, in turn, enforces stronger serializability
// semantics, since we won't have a case where one peer may have a higher LastLoggedTxid while another has a higher 
// LastCommittedTxid.
//
func (l *Leader) handleMessage(msg common.Packet, follower string) (err error) {

	err = nil
	switch request := msg.(type) {
	case RequestMsg:
		err = l.CreateProposal(follower, request)
	case AcceptMsg:
		err = l.handleAccept(request)
	default:
		// TODO: Should throw exception.  There is a possiblity that there is another leader.   
		log.Printf("Leader.handleMessage(): Leader unable to process message of type %s. Ignore message.", request.Name())
	}
	return err 
}

/////////////////////////////////////////////////////////////////////////
// Leader - Private Function : Handle Request Message  (New Proposal)
/////////////////////////////////////////////////////////////////////////

//
// Create a new proposal from request
//
func (l *Leader) CreateProposal(host string, req RequestMsg) error {

	// This should be the only place to call GetNextTxnId().  This function
	// can panic if the txnid overflows.   In this case, this should terminate
	// the leader and forces a new election for getting a new epoch. ZK has the
	// same behavior.
	txnid := common.GetNextTxnId()
	log.Printf("Leader.CreateProposal(): New Proposal : Epoch %d, Counter %d", 
			txnid.GetEpoch(), txnid.GetCounter())

	// Create a new proposal	
	proposal := l.factory.CreateProposal(uint64(txnid),
		host,		// this is the host the originates the request
		req.GetReqId(),
		req.GetOpCode(),
		req.GetKey(),
		req.GetContent())

	return l.NewProposal(proposal)
}

//
// Handle a new proposal
//
func (l *Leader) NewProposal(proposal ProposalMsg) error {

	// Call out to log the proposal.  Always do this first before
	// sending to followers.
	err := l.handler.LogProposal(proposal)
	if err != nil {
		// If fails to log the proposal, return the error. 
		// This can cause the leader to re-elect.  Just to handle 
		// case where there is hardware failure or repository
		// corruption.
		return err
	}

	// The leader votes for itself 
	l.updateQuorum(common.Txnid(proposal.GetTxnid()), l.GetFollowerId())
	
	// remember this proposal so I can commmit it later
	l.proposals[common.Txnid(proposal.GetTxnid())] = proposal

	// Send the proposal to follower
	l.sendProposal(proposal)
	
	// check if proposal has quorum (if ensembleSize <= 2).  Make sure that this 
	// is after sendProposal() so that we can still send proposal to follower BEFORE 
	// we send the commit message.
	if l.hasQuorum(common.Txnid(proposal.GetTxnid())) {
		// proposal has quorum. Now commit. If cannot commit, then return error
		// which will cause the leader to re-election.  Just to handle 
		// case where there is hardware failure or repository corruption.
		err := l.commit(common.Txnid(proposal.GetTxnid()))
		if err != nil {
			return err
		}
	}
	
	return nil
}

//
// send the proposal to the followers
//
func (l *Leader) sendProposal(proposal ProposalMsg) {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	// Send the request to the followers.  Each follower
	// has a pipe (channel) and there is separate go-routine
	// that will read from the channel and send to the follower
	// through reliable connection (TCP).   If the connection is
	// broken, then the follower will go through election again,
	// and the follower will re-sync the repository with the leader.
	// Therefore, we don't have to worry about re-sending proposal
	// to disconnected follower here.   
	//
	// If the leader cannot send to a follower, then pipe will
	// be broken and the leader will take the follower out.  If
	// the leader looses majority of followers, it will bring
	// itself into election again.    So we can process this
	// asynchronously without worrying about sending the message 
	// to majority of followers.  If that can't be done, the
	// leader re-elect.
	//
	msg := l.factory.CreateProposal(proposal.GetTxnid(),
			proposal.GetFid(),
			proposal.GetReqId(),
			proposal.GetOpCode(),
			proposal.GetKey(),
			proposal.GetContent())
			
	for _, f := range l.followers {
		f.pipe.Send(msg)
	}

	for _, w := range l.watchers {
		w.Send(msg)
	}
	
	for _, o := range l.observers {
		o.send(msg)
	}
}

/////////////////////////////////////////////////////////
// Leader - Private Function : Handle Accept Message
/////////////////////////////////////////////////////////

//
// handle accept message from follower
//
func (l *Leader) handleAccept(msg AcceptMsg) error {

	// If this Ack is on an old proposal, then ignore.
	// This indicates that the follower may be slower
	// than others.  Therefore, the proposal may be
	// committed, before the follower can Ack.
	mtxid := common.Txnid(msg.GetTxnid())
	if l.lastCommitted >= mtxid {
		// cleanup.  l.quorums should not have mtxid.  
		// This is just in case since we will never commit
		// this proposal.
		_, ok := l.quorums[mtxid]
		if ok {
			delete(l.quorums, mtxid)
			delete(l.proposals, mtxid)
		}
		return nil
	}

	// update quorum
	l.updateQuorum(mtxid, msg.GetFid())
	if l.hasQuorum(mtxid) {
		// proposal has quorum. Now commit. If cannot commit, then return error
		// which will cause the leader to re-election.  Just to handle 
		// case where there is hardware failure or repository corruption.
		err := l.commit(mtxid)
		if err != nil {
			return err
		}
	}
	
	return nil
}

//
// update quorum of proposal
//
func (l *Leader) updateQuorum(txid common.Txnid, fid string) {

	if l.quorums[txid] == nil {
		l.quorums[txid] = make([]string, 0, common.MAX_FOLLOWERS)
	}
	log.Printf("Leader.updateQuorum: current quorum for txid %d : %d", uint64(txid), len(l.quorums[txid]))

	// Just to double check if the follower has already voted on this proposal.
	var found bool
	for i := 0; i < len(l.quorums[txid]); i++ {
		a := l.quorums[txid][i]
		if a == fid {
			found = true
			break
		}
	}

	if !found {
		l.quorums[txid] = append(l.quorums[txid], fid)
	}
	
	log.Printf("Leader.updateQuorum: new quorum for txid %d : %d", uint64(txid), len(l.quorums[txid]))
}

//
// check if a proposal has reached quorum
//
func (l *Leader) hasQuorum(txid common.Txnid) bool {
	// This uses a simple majority of quorum.  ZK also has a
	// hierarchy quorums for scalability (servers are put into different
	// groups and quorums are obtained within a group).

	accepted, ok := l.quorums[txid]
	if !ok {
		// no one has voted yet on this proposal 
		return false
	}

	return uint64(len(accepted)) > (l.handler.GetEnsembleSize()/2)
}

//
// commit proposal
//
func (l *Leader) commit(txid common.Txnid) error {

	// We are skipping proposal.  The protocol expects that each follower must
	// send Accept Msg in the order of Proposal being received.   Since the
	// message is sent out a reliable TCP connection, it is not possible to reach
	// quorum out of order.  Particularly, if a new follower leaves and rejoins,
	// the leader is responsible for resending all the pending proposals to the
	// followers.   So if we see the txid is out-of-order there, then it is
	// a fatal condition due to protocol error. 
	//
	if !common.IsNextInSequence(txid, l.lastCommitted) {
		return common.NewError(common.PROTOCOL_ERROR, 
			fmt.Sprintf("Proposal must committed in sequential order for the same leader term. " +
			"Found out-of-order commit. Leader last committed txid %d, commit msg %d", l.lastCommitted, txid))
	}
	
	_, ok := l.proposals[txid]
	if !ok {
		return common.NewError(common.SERVER_ERROR, 
			fmt.Sprintf("Cannot find a proposal for the txid %d. Fail to commit the proposal.", txid))
	}

	// marking the proposal as committed.  Always do this first before sending to followers. 
	err := l.handler.Commit(txid)
	if err != nil {
		return err
	}

	// remove the votes
	delete(l.quorums, txid)
	delete(l.proposals, txid)

	// Update lastCommitted 
	l.lastCommitted = txid
	
	// Send the commit to followers
	l.sendCommit(txid)
	
	// TODO: do we need to update election site?  I don't think so, but need to double check.
	
	return nil
}

//
// send commit messages to all followers
//
func (l *Leader) sendCommit(txnid common.Txnid) error {

	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	msg := l.factory.CreateCommit(uint64(txnid))
	
	// Send the request to the followers.  See the same
	// comment as in sendProposal()
	//
	for _, f := range l.followers {
		f.pipe.Send(msg)
	}

	// send message to watchers
	for _, w := range l.watchers {
		w.Send(msg)
	}
	
	// Send the message to the observer
	for _, o := range l.observers {
		o.send(msg)
	}
	
	return nil
}

