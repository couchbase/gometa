package protocol

import (
	"net"
	"time"
	"sync"
	"common"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type CompareResult byte
const (
	EQUAL CompareResult = iota
  	GREATER	
  	LESSER	
)

//
// The ElectionSite controls all the participants of a election.
// 1) messenger - repsonsible for sending messages to other voter
// 2) ballot master - manages a ballot orginated from this node.   This includes
//    re-balloting if there is no convergence on the votes. 
// 3) poll worker - recieve votes from other voters and determine if majority is reached
//
type ElectionSite struct {
	messenger       *common.PeerMessenger
	master  		*BallotMaster
	worker			*PollWorker
	ensemble         []net.Addr
	isClosed         bool
	mutex            sync.Mutex
	factory          MsgFactory
	handler          ActionHandler
}

type BallotResult struct {
	proposed        VoteMsg
	receivedVotes   map[string]VoteMsg
	activePeers     map[string]VoteMsg
}

type Ballot struct {
	result			*BallotResult
	resultch         chan bool  // should only be closed by PollWorker
}

type BallotMaster struct {
	site	         *ElectionSite	
	winner           *BallotResult
	round			 uint64
	mutex            sync.Mutex
	inProg			 bool
}

type PollWorker struct {
	site			 *ElectionSite	
	ballot           *Ballot
	listench         chan *Ballot 
	killch           chan bool 
}

/////////////////////////////////////////////////////////////////////////////
// ElectionSite 
/////////////////////////////////////////////////////////////////////////////

//
// Create ElectionSite
// 
func CreateElectionSite(laddr string, 
						peers []net.Addr, 
						factory MsgFactory,
						handler ActionHandler) (election *ElectionSite, err error) {

	election = &ElectionSite{isClosed : false,
							 factory : factory,
							 handler : handler,
							 ensemble : peers}

	// Create a new messenger
	election.messenger, err = newMessenger(laddr)
	if err != nil {
		return nil, err
	}
	
	// Create a new ballot master	
	election.master = newBallotMaster(election)

	// Create a new poll worker.  This will start the
	// goroutine for the pollWorker.
	election.worker = newPollWorker(election)
	
	return election, nil
}

//
// Start a new Election.  If there is a ballot in progress, this function
// will return false.  The ballot will happen indefinitely until a winner
// merge.   The winner will be returned through winnerch.
//
func (e *ElectionSite) StartElection(winnerch chan string) bool {

	// ballot in progress
	if !e.master.setBallotInProg(false) || e.IsClosed() {
		return false
	}

	go e.master.castBallot(winnerch)
	
	return true
}

//
// Close ElectionSite.  Any pending ballot will be closed immediately.
// This will also terminate the ability as a voter.
//
func (e *ElectionSite) Close() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	
	if !e.isClosed {
		e.isClosed = true
		
		e.messenger.Close()	
		e.master.close()
		e.worker.close()
	}
}

//
// Tell if the ElectionSite is closed.
//
func (e *ElectionSite) IsClosed() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	
	return e.isClosed
}

//
// Create Vote from State
//
func (s *ElectionSite) createVoteFromCurState() VoteMsg {

	return s.factory.CreateVote(s.master.round, 
								uint32(s.handler.GetStatus()),
								uint32(s.handler.GetCurrentEpoch()),
	                            s.messenger.GetLocalAddr(), 
	                            uint64(s.handler.GetLastLoggedTxid()))
}

//
// Tell if a particular voter is in the ensemble
//
func (s *ElectionSite) inEnsemble(voter net.Addr) bool {
	for _, peer := range s.ensemble {
		if peer.String() == voter.String() {
			return true
		}
	}
	return false
}

/////////////////////////////////////////////////////////////////////////////
// BallotMaster 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new BallotMaster. 
//
func newBallotMaster(site *ElectionSite) *BallotMaster {

	master := &BallotMaster{site : site,
						    winner : nil,
						    round : 0,
	                        inProg : false}
	                    
	return master
}

//
// Start a new round of ballot.  
//
func (b* BallotMaster) castBallot(winnerch chan string) {

	// create a channel to receive the ballot result 
	// should only be closed by Poll Worker
	resultch := make(chan bool)
	
	// Create a new ballot 
	ballot := b.createInitialBallot(resultch)
			
	// Tell the worker to observe this ballot.  This forces
	// the worker to start collecting new ballot result.
	b.site.worker.observe(ballot)
	
	// let the peer to know about this ballot.  It is expected
	// that the peer will reply with a vote.  
	b.site.messenger.Multicast(ballot.result.proposed, b.site.ensemble)	                      
		
	success, ok := <- resultch
	if !ok {
		// channel close. Ballot done
		success = false
	} 
	
	// Announce the winner 
	if success {
		common.SafeRun("BallotMaster.castBallot()", 
							func() {
								winnerch <- b.winner.proposed.GetCndId()
							})
	} else {
		// close the winnerch if we cannot finish the ballot.
		common.SafeRun("BallotMaster.castBallot()", 
							func() {
								close(winnerch)
							})
	}
							
    // balloting complete							
	b.setBallotInProg(true)
}

//
// close the ballot master.
//
func (b *BallotMaster) close() {
	// Nothing to do now.   The current ballot
	// is closed when the ballot resultch is closed.
	// Instead of doing it in this method, should
	// do it in the poll worker to avoid race condition.
}

//
// if there is a balllot in progress 
//
func (b *BallotMaster) setBallotInProg(clear bool) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	if b.inProg && !clear {
		return false
	}
	
	if !b.inProg && clear {
		return false
	}
	
	if clear {
		b.inProg = false
	} else {
		b.inProg = true
	}
	
	return true 
}

//
// Get the next id for ballot.
//
func (b *BallotMaster) getNextRound() uint64 {

  	result := b.round
  	b.round++
  	return result
}

//
// Create a ballot 
//
func (b *BallotMaster) createInitialBallot(resultch chan bool) *Ballot {

	result := &BallotResult{receivedVotes : make(map[string]VoteMsg),
						    activePeers : make(map[string]VoteMsg)}

	b.getNextRound()
	newVote := b.site.createVoteFromCurState()
	
	laddr := b.site.messenger.GetLocalAddr()
	result.receivedVotes[laddr] = newVote 
	result.proposed = newVote
	
	ballot := &Ballot{result : result, 
					  resultch : resultch} 
					
	return ballot
}

//
// Copy a winning vote
//
func (b *BallotMaster) cloneWinningVote() VoteMsg {
	if b.winner != nil {
		return b.site.factory.CreateVote(b.round,
								uint32(b.site.handler.GetStatus()),	
								uint32(b.site.handler.GetCurrentEpoch()),	
								b.winner.proposed.GetCndId(),
								b.winner.proposed.GetCndTxnId())		
	}
	
	return nil
}

//
// Set the current round.  This function is there just for
// easier to keep track of different places that set 
// the BallotMaster.round.   BallotMaster.round should
// always be in sycn with the ballot.result.proposed.round or
// the master.winner.proposed.round.
//
func (b *BallotMaster) setCurrentRound(round uint64) {
	b.round = round
}

/////////////////////////////////////////////////////////////////////////////
// Ballot 
/////////////////////////////////////////////////////////////////////////////

//
// Reset the ballot
//
func (b *Ballot) reset(proposed VoteMsg) {
	b.result.proposed = proposed
	b.result.receivedVotes = make(map[string]VoteMsg)
	
	// Do not reset activePeers.  This should not affect
	// correctness if reset activePeers, but since 
	// activePeers are already either leader/follower,
	// they won't change their vote. 
}

/////////////////////////////////////////////////////////////////////////////
// PollWorker 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new PollWorker.  The PollWorker listens to the Vote receving from
// the peers for a particular ballot.
//
func newPollWorker(site *ElectionSite) *PollWorker {

	worker := &PollWorker{site : site,
						  ballot : nil,
						  killch : make(chan bool),
	                      listench : make(chan *Ballot)}
	     
	go worker.listen()                      
	
	return worker
}

//
// Close the PollWorker
//
func (p *PollWorker) close() {
	p.killch <- true
}

//
// Goroutine.  Listen to vote coming from the peer for a
// particular ballot.  This is the only goroutine that
// handle all incoming requests.
//
// Voter -> the peer that replies the ballot with a vote
// Candidate -> the peer that is voted for by the voter.  
// It is the peer (CndId) that is inside the vote. 
//
func (w *PollWorker) listen() {

	// Get the channel for receiving votes from the peer.
	reqch := w.site.messenger.DefaultReceiveChannel()
	duration := common.BALLOT_TIMEOUT 
	timeout := time.After(duration * time.Millisecond)	
	
	for {
		select {
			case w.ballot = <- w.listench : 
			{
				// There is a new ballot.
	            duration := common.BALLOT_TIMEOUT 
				timeout = time.After(duration * time.Millisecond)	
			}
			// Receiving a vote 
			case msg, ok := <- reqch :	
			{
				if !ok {
					// TODO: Channel closed.  
					return 
				}
				
				var obj interface{} = msg.Content
				vote := obj.(VoteMsg)
				voter := msg.Peer
				
				// Check if the voter is in the ensemble
				if !w.site.inEnsemble(voter) {
					continue
				}
				
				if w.ballot == nil {
					// If there is no ballot, then just need to respond.
					w.respondInquiry(voter, vote)
					
				} else if w.handleVote(voter, vote) {
					// we achieve quorum, set the winner.  
					// setting the winner and usetting the ballot
					// should be done together.
		            w.site.master.winner = w.ballot.result
					w.ballot.resultch <- true 
	              	w.ballot = nil
				}
				
				duration = common.BALLOT_TIMEOUT
				timeout = time.After(duration * time.Millisecond)	
			}		
			case <- timeout :
			{
				// If there is a timeout but no response,
				// send vote again
				if w.ballot != nil {
                	w.site.messenger.Multicast(w.cloneProposedVote(), w.site.ensemble)	                      
				}
				
				duration = duration * 2	
				timeout = time.After(duration * time.Millisecond)	
			}
			case <- w.killch :
			{
				// It is done.  Close the ballot.
				common.SafeRun("PollWorker.gatherVote()", 
							func() {
								close(w.ballot.resultch)
								if w.ballot != nil {
									w.ballot = nil
								}
							})
				return	
			}
		}
	}
}

//
// The PollWorker is no longer in election.  Respond to inquiry from
// the peer.
//
func (w *PollWorker) respondInquiry(voter net.Addr, vote VoteMsg) {

	if PeerStatus(vote.GetStatus()) == ELECTING {
		if w.site.master.winner != nil {
        	w.site.messenger.Send(w.site.master.cloneWinningVote(), voter)	 
        }
        // If there is no winner at the moment and this node is not
        // in election, could have send a vote based on the current state 
        // (lastLoggedZxid).  But it is expected that a new election will be 
        // started for this node soon, so don't have to send it now.  
	}
}

//
// Handle a new vote.  
//
func (w *PollWorker) handleVote(voter net.Addr, vote VoteMsg) bool {

	if PeerStatus(vote.GetStatus()) == ELECTING {
		// if peer is still in election
		return w.handleVoteForElectingPeer(voter, vote)
	} else {
		// if peer is either leading or following 
		return w.handleVoteForActivePeer(voter, vote)
	}
}

//
// Handle a new vote if peer is electing.  
//
func (w *PollWorker) handleVoteForElectingPeer(voter net.Addr, vote VoteMsg) bool {

	// compare the round 
	compareRound := w.compareRound(vote)
			
	// if the incoming vote has a greater round, re-ballot.
	if compareRound == GREATER {
		// I am behing the ballot round. Update myself.
		// BallotMaster.round should always be in sync with 
		// ballot.result.proposed.round
		w.site.master.setCurrentRound(vote.GetRound())
						
	 	if w.compareVoteWithLastLogged(vote) == GREATER {
		    // Update my vote if the incoming vote is larger.
			w.ballot.reset(vote)
	 	} else {
			// otherwise udpate my vote using lastLoggedTxid 
			vote = w.site.createVoteFromCurState()
			w.ballot.reset(vote)
	 	}	
					 
	 	// notify that our new vote	
        w.site.messenger.Multicast(w.cloneProposedVote(), w.site.ensemble)	                      
		               
       	// if we reach quorum with this vote, announce the result 
      	// and stop election
        return w.acceptAndCheckQuorum(voter, vote) 
		
	} else if compareRound == EQUAL {
		// if it is the same round and the incoming vote has higher txid,  
		// update myself to the incoming vote and broadcast my new vote
	 	if w.compareVoteWithProposed(vote) == GREATER {
			w.ballot.result.proposed = vote
			
	 		// notify that our new vote	
           	w.site.messenger.Multicast(w.cloneProposedVote(), w.site.ensemble)	                      

			// Add this vote to the received list.  Note that even if
			// the peer went down there is network partition after the
			// vote is being sent by peer, we still count this vote.
			// If somehow we got the wrong leader because of this, we
			// not be able to finish in the discovery/sync phase anyway,
			// and a new election will get started.
       		
       		// If I believe I am chosen as a leader in the election 
       		// and the network is partitioned afterwards.  The 
       		// sychonization phase will check if I do get a majorty 
       		// of followers connecting to me before proceeding.  So 
       		// for now, I can return as long as I reach quorum and
       		// let subsequent phase to do more checking.
       		
	       	return w.acceptAndCheckQuorum(voter, vote) 
		}
	} else {
		// send back the notification to the sender with my round
        w.site.messenger.Send(w.cloneProposedVote(), voter)	 
	}	
	
	return false
}

//
// Handle a new vote from a leader or follower  
//
func (w *PollWorker) handleVoteForActivePeer(voter net.Addr, vote VoteMsg) bool {

	// compare the round 
	compareRound := w.compareRound(vote)
	
	if compareRound == EQUAL {
		// I am receiving a vote from a leader/follower, they
		// may be responding to my vote.  They probably know
		// the current leader.  So we really should be expecting
		// getting a message from the leader (unless leader is me).
		w.ballot.result.receivedVotes[voter.String()] = vote
		
		if w.checkQuorum(w.ballot.result.receivedVotes, vote)
		   w.certifyLeader(vote) {
		   return true
	    }
	}

	// The active peer has chosen a leader, but we cannot confirm it yet.
	// Keep the active peer onto a different list, since receivedVotes
	// can be reset.  If this peer goes down after sending us his vote,
	// his vote still count in this ballot.  By calling certifyLeader(), 
	// we can also makes sure that the candidate has established itself to 
	// us as a leader. 
	w.ballot.result.activePeers[voter.String()] = vote
		 
	if w.checkQuorum(w.ballot.result.activePeers, vote) &&
	   w.certifyLeader(vote) {
	   // my master.round may be ahead of the leader since
	   // I am in election.  The leader's ballot round would
	   // not change when it is not in election.  So change
	   // mine to match the leader's ballot round.
	   w.site.master.setCurrentRound(vote.GetRound())
	   return true
	}
	
	return false
}

//
// Notify the PollWorker that there is a new ballot.
//
func (w *PollWorker) observe(ballot *Ballot) {
	// This synchronous.  This is to ensure that listen() receives the ballot 
	// before this function return to the BallotMaster.
	w.ballot = ballot
	w.listench <- ballot 
}

//
// Compare the current round with the given vote 
//
func (w *PollWorker) compareRound(vote VoteMsg) CompareResult {

	currentRound := w.site.master.round
	
	if vote.GetRound() == currentRound {
		return EQUAL 
	}
	
	if vote.GetRound() > currentRound {
		return GREATER 
	}
	
	return LESSER 
}

//
// Compare two votes.  Return true if vote1 is larger than vote2.
//
func (w *PollWorker) compareVote(vote1, vote2 VoteMsg) CompareResult {
	if vote1.GetEpoch() > vote2.GetEpoch() {
		return GREATER 
	}
	
	if vote1.GetCndTxnId() > vote2.GetCndTxnId() {
		return GREATER 
	}
	
	if vote1.GetCndId() > vote2.GetCndId() {
		return GREATER 
	}
	
	if vote1.GetEpoch() < vote2.GetEpoch() {
		return LESSER 
	}
	
	if vote1.GetCndTxnId() < vote2.GetCndTxnId() {
		return LESSER 
	}
	
	if vote1.GetCndId() < vote2.GetCndId() {
		return LESSER
	}
	
	return EQUAL 
}

//
// Compare the given vote with last logged Txid
//
func (w *PollWorker) compareVoteWithLastLogged(vote VoteMsg) CompareResult {

	vote2 := w.site.createVoteFromCurState()
	return w.compareVote(vote, vote2)
}

//
// Compare the given vote with proposed vote 
//
func (w *PollWorker) compareVoteWithProposed(vote VoteMsg) CompareResult {

	return w.compareVote(vote, w.ballot.result.proposed)
}

//
// Accept the check quorum 
//
func (w *PollWorker) acceptAndCheckQuorum(voter net.Addr, vote VoteMsg) bool {

	w.ballot.result.receivedVotes[voter.String()] = vote
	return w.checkQuorum(w.ballot.result.receivedVotes, w.ballot.result.proposed)

	//TODO: After quorum is reached, TK will wait to see if there is any additional
	//messages that will change leader.  It is possibly an optimization because if
	//it is a wrong leader, it will fail during discovery/sync and force a new
	//re-election.
}

// 
// Check Quorum
//
func (w *PollWorker) checkQuorum(votes map[string]VoteMsg, candidate VoteMsg) bool {

	count := 0	
	for _, vote := range votes {
		if 	PeerStatus(vote.GetStatus()) == ELECTING ||
			PeerStatus(candidate.GetStatus()) == ELECTING {
			if w.compareVote(vote, candidate) == EQUAL &&
			   vote.GetRound() == candidate.GetRound() {
			   count++
			}			
		} else if vote.GetCndId() == candidate.GetCndId() &&
		          vote.GetEpoch() == candidate.GetEpoch() {  
		    count++
		} 
	}	
	
	return count > (len(w.site.ensemble) / 2)
}	

//
// Copy a proposed vote
//
func (w *PollWorker) cloneProposedVote() VoteMsg {
	// w.site.master.round should be in sycn with 
	// w.ballot.result.proposed.round. Use w.site.master.round
	// to be consistent.
	return w.site.factory.CreateVote(w.site.master.round, 
								uint32(w.site.handler.GetStatus()),
								uint32(w.site.handler.GetCurrentEpoch()),
								w.ballot.result.proposed.GetCndId(),
								w.ballot.result.proposed.GetCndTxnId())		
}

//
// Certify the leader before declaring followship
//
func (w *PollWorker) certifyLeader(vote VoteMsg) bool {

	// I am not voted as leader
	if vote.GetCndId() != w.site.messenger.GetLocalAddr() {
		// 	The leader must be known to me as active
		leaderVote, ok := w.ballot.result.activePeers[vote.GetCndId()]
		if ok && PeerStatus(leaderVote.GetStatus()) == LEADING {
			return true	
		}
		return false
	} 

	// If someone voting me as a leader, make sure that we have the same round	
	return w.site.master.round == vote.GetRound() 
}

/////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func newMessenger(laddr string) (*common.PeerMessenger, error) {

    conn, err := common.GetElectionConn(laddr) 
    if err != nil {
    	return nil, err
    }
    
    messenger , err := common.NewPeerMessenger(conn, nil)
    if err != nil {
    	return nil, err
    }
    
    return messenger, nil
}
