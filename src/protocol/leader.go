package protocol

import (
	"common"
	"sync"
	"log"
)

/////////////////////////////////////////////////
// Proposal Processing for ZK
/////////////////////////////////////////////////

// 1) TCP is used as the network protocol (guarantee packet ordering)
// 2) The leader sends the proposal sequentially based on the order of
//    received requests.  Due to (1), it expects the proposal to be
//    arrived at each follower in the same order.
// 3) Each follower processes the proposal in the same sequential order
//    Therefore, the ack is returned in the same order as the proposal.
// 4) In the follower, proposal and commit can be processed out-of-order.
//    These 2 messages go through different queues on the follower side.
// 5) The leader calls the LearnerHandler (a separate go-routine) to
//    send/recieve proposal/commit for a specific follower.  If that
//    fails,  it will close the socket.  This, in turn, will terminate
//    both the sending and recieving threads of the LearnerHandler and
//    force the LearnerHandler to shutdown.  In  doing so, the leader will
//    also remove the follower.  The leader will listen to any new socket
//    connection to re-estabilish communication with the follower.
// 6) When the follower fails to send a Ack to the leader, it will close the socket.
//    This, in turn, will shutdown the follower.  The thread (QuorumPeer) will
//    continue to run in "looking" state.  While at "looking" state, it will execute
//    the leader election algorithm to find the new leader.
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
	naddr         string
	lastCommitted common.Txnid
	handler       ActionHandler
	factory       MsgFactory
	quorums       map[common.Txnid][]string

	// mutex protected variable
	mutex     sync.Mutex
	followers map[string]*common.PeerPipe
	pendings  []ProposalMsg
}

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Create a new leader
//
func NewLeader(naddr string,
	handler ActionHandler,
	factory MsgFactory) *Leader {
	leader := &Leader{naddr: naddr,
		followers:     make(map[string]*common.PeerPipe),
		pendings:      make([]ProposalMsg, 0, common.MAX_PROPOSALS),
		lastCommitted: 0,
		quorums:       make(map[common.Txnid][]string),
		handler:       handler,
		factory:       factory}
	return leader
}

//
// Terminate the leader
//
func (l *Leader) Terminate() {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, pipe := range l.followers {
		pipe.Close()
	}
}

/////////////////////////////////////////////////
// Follower Listener
/////////////////////////////////////////////////

//
// Add a follower and starts a listener for the follower
//
func (l *Leader) AddFollower(peer *common.PeerPipe) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.followers[peer.GetAddr()] = peer

	// The listener is terminated when the leader close the pipe
	// (in Terminate() method).
	go l.startListener(peer)
}

//
// Gorountine.  Start listener to listen to message from follower.
// Note that each follower has their own receive queue.  This
// is to ensure if the queue is filled up for a single follower,
// only that the connection of that follower may get affected.
//
func (l *Leader) startListener(follower *common.PeerPipe) {

	log.Printf("Leader.startListener(): start listening to message from peer %s", follower.GetAddr())
	reqch := follower.ReceiveChannel()

	for {
		req, ok := <-reqch
		if ok {
			// TODO : handle error
			l.handleMessage(req, follower.GetAddr())
		} else {
			// The channel is closed.  Need to shutdown the listener.
			log.Printf("Leader.startListener(): message channel closed. Remove peer %s as follower.", follower.GetAddr()) 
			l.removeFollower(follower)
			return
		}
	}
}

//
// Remove the follower from being tracked
//
func (l *Leader) removeFollower(peer *common.PeerPipe) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	delete(l.followers, peer.GetAddr())
}

/////////////////////////////////////////////////
// Message Processing
/////////////////////////////////////////////////

//
// Main entry point for processing messages from followers
//
func (l *Leader) handleMessage(msg common.Packet, follower string) (err error) {

	// TODO: Parallelize RequesMsg independently from AcceptMsg
	l.mutex.Lock()
	defer l.mutex.Unlock()

	switch request := msg.(type) {
	case RequestMsg:
		// TODO: handle error
		l.CreateProposal(follower, request)
	case AcceptMsg:
		err = l.handleAccept(request)
		if err != nil {
			return err
		}
	default:
		// TODO : if we don't recoginize the message.  Just log it and ignore.
	}
	return nil
}

/////////////////////////////////////////////////
// Handle Request Message  (New Proposal)
/////////////////////////////////////////////////

//
// Create a new proposal from request
//
func (l *Leader) CreateProposal(host string, req RequestMsg) error {

	// Create a new proposal.  Note that there is a possibility that
	// GetNextTnxId() returns a id that is overflow.  In this case,
	// the epoch portion of the TxnId will be incremented.  In other
	// words, the leader will be automatically granted a new term.

	// TODO : In ZK, when the id is overflow, the server is being restarted. Check
	//        if this implementation is safe for leader election.
	txnid := l.handler.GetNextTxnId()
	proposal := l.factory.CreateProposal(uint64(txnid),
		host,
		req.GetReqId(),
		req.GetOpCode(),
		req.GetKey(),
		req.GetContent())

	l.NewProposal(proposal)

	return nil
}

//
// Handle a new proposal
//
func (l *Leader) NewProposal(proposal ProposalMsg) {

	// Keep track of the pending proposal
	l.pendings = append(l.pendings, proposal)

	// TODO : Call out to log the proposal
	// This deviates from ZK where the proposal is sent to followers before
	// logging.  By logging first, this approach is more aligned with RAFT.

	// TODO: Send an Ack to the leader itself.  In ZK, leader can vote.

	// Send the proposal to follower
	l.sendProposal(proposal)
}

//
// send the proposal to the followers
//
func (l *Leader) sendProposal(proposal ProposalMsg) {

	// Send the request to the followers
	for _, f := range l.followers {
		msg := l.factory.CreateProposal(proposal.GetTxnid(),
			proposal.GetFid(),
			proposal.GetReqId(),
			proposal.GetOpCode(),
			proposal.GetKey(),
			proposal.GetContent())
		f.Send(msg)
	}
}

/////////////////////////////////////////////////
// Handle Accept Message
/////////////////////////////////////////////////

//
// handle accept message from follower
//
func (l *Leader) handleAccept(msg AcceptMsg) error {

	// This can happen if the follower is slow when
	// the proposal has reached quorum before the follower
	// can accept.
	if len(l.pendings) == 0 {
		return nil
	}

	// If this Ack is on an old proposal, then ignore.
	// This indicates that the follower may be slower
	// than others.  Therefore, the proposal may be
	// committed, before the follower can Ack.
	mtxid := common.Txnid(msg.GetTxnid())
	if l.lastCommitted >= mtxid {
		return nil
	}

	// look for the proposal with the matching txnid
	for i := 0; i < len(l.pendings); i++ {
		p := l.pendings[i]
		if common.Txnid(p.GetTxnid()) == mtxid {
			l.updateQuorum(mtxid, msg.GetFid())
			if l.hasQuorum(mtxid) {
				l.commit(mtxid, p)
				break
			}
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
}

//
// check if a proposal has reached quorum
//
func (l *Leader) hasQuorum(txid common.Txnid) bool {
	// This uses a simple majority of quorum.  ZK also has a
	// hierarchy quorums for scalability (servers are put into different
	// groups and quorums are obtained within a group).

	// TODO: The implementation assumes the number of followers are stable.
	// Need to work on variable number of followers.

	accepted := l.quorums[txid]
	if accepted == nil {
		// we need at least one follower to accept.
		return false
	}

	return len(accepted) >= (len(l.followers)/2 + 1)
}

//
// commit proposal
//
func (l *Leader) commit(txid common.Txnid, p ProposalMsg) {

	// We are skipping proposal.  The protocol expects that each follower must
	// send Accept Msg in the order of Proposal being received.   Since the
	// message is sent out a reliable TCP connection, it is not possible to reach
	// quorum out of order.  Particularly, if a new follower leaves and rejoins,
	// the leader is responsible for resending all the pending proposals to the
	// followers.   So if we see the txid is out-of-order there, then it is
	// a fatal condition due to protocol error.
	if txid != l.lastCommitted+1 {
		// TODO: log warning
	}

	// remove the proposal
	// TODO: remove proposals that are older than txid, since
	// those proposals should never be committed.
	for i := 0; i < len(l.pendings); i++ {
		p := l.pendings[i]
		if common.Txnid(p.GetTxnid()) == txid {
			// TODO: Make it more efficient
			// TODO: if i == 0
			dst := make([]ProposalMsg, 0, common.MAX_PROPOSALS)
			dst = append(dst, l.pendings[0:i]...)
			dst = append(dst, l.pendings[i+1:]...)
			l.pendings = dst
			break
		}
	}
	delete(l.quorums, txid)

	// send Commit to followers
	l.lastCommitted = txid
	l.sendCommit(txid)

	// handle the commit action
	err := l.handler.Commit(p)
	if err != nil {
		// TODO throw error
	}

	// TODO: send response to client
	// TODO: update election site
}

//
// send commit messages to all followers
//
func (l *Leader) sendCommit(txnid common.Txnid) error {

	msg := l.factory.CreateCommit(uint64(txnid))

	// TODO: check if we need to send to all followers or
	// just the follower which responds
	// Send the request to the followers
	for _, f := range l.followers {
		f.Send(msg)
	}

	return nil
}
