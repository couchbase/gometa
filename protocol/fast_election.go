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

package protocol

import (
	"github.com/couchbase/gometa/common"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

//
// The ElectionSite controls all the participants of a election.
// 1) messenger - repsonsible for sending messages to other voter
// 2) ballot master - manages a ballot orginated from this node.   This includes
//    re-balloting if there is no convergence on the votes.
// 3) poll worker - recieve votes from other voters and determine if majority is reached
//
type ElectionSite struct {
	messenger *common.PeerMessenger
	master    *ballotMaster
	worker    *pollWorker

	solicitOnly  bool
	ensemble     []net.Addr
	fullEnsemble []string
	factory      MsgFactory
	handler      ActionHandler

	mutex    sync.Mutex
	isClosed bool
}

type ballotResult struct {
	proposed      VoteMsg
	winningEpoch  uint32             // winning epoch : can be updated after follower has sync with leader
	receivedVotes map[string]VoteMsg // the map key is voter UDP address
	activePeers   map[string]VoteMsg // the map key is voter UDP address
}

type Ballot struct {
	result   *ballotResult
	resultch chan bool // should only be closed by pollWorker
}

type ballotMaster struct {
	site *ElectionSite

	// mutex protected state
	mutex  sync.Mutex
	winner *ballotResult
	inProg bool
	round  uint64
}

type pollWorker struct {
	site     *ElectionSite
	ballot   *Ballot
	listench chan *Ballot
	killch   chan bool
}

//
// The election round is incremented for every new election being run in
// this process.    If there is an ensemble of peers are running election,
// these peers will need to be in the same round in order to achieve quorum.
// Essentially, if a peer joins an electing ensemble, it can either join
// the current round of voting or start a new round.  If it start a new round,
// then it must have enough peers to join his round before a quorum can be reached.
// If a peer leaves an ensemble, its vote still count (ZK does not take away vote).
// The sycnhronization (recovery) phase will double check if a quorum of followers
// agree to the leader before the algorithm is fully converged.
//
var gElectionRound uint64 = 0

/////////////////////////////////////////////////////////////////////////////
// ElectionSite (Public API)
/////////////////////////////////////////////////////////////////////////////

//
// Create ElectionSite
//
func CreateElectionSite(laddr string,
	peers []string,
	factory MsgFactory,
	handler ActionHandler,
	solicitOnly bool) (election *ElectionSite, err error) {

	// create a full ensemble (including the local host)
	en, fullEn, err := cloneEnsemble(peers, laddr)
	if err != nil {
		return nil, err
	}

	election = &ElectionSite{isClosed: false,
		factory:      factory,
		handler:      handler,
		ensemble:     en,
		fullEnsemble: fullEn,
		solicitOnly:  solicitOnly}

	// Create a new messenger
	election.messenger, err = newMessenger(laddr)
	if err != nil {
		return nil, err
	}

	// Create a new ballot master
	election.master = newBallotMaster(election)

	// Create a new poll worker.  This will start the
	// goroutine for the pollWorker.
	election.worker = startPollWorker(election)

	return election, nil
}

//
// Start a new Election.  If there is a ballot in progress, this function
// will return a nil channel.  The ballot will happen indefinitely until a winner
// emerge or there is an error.   The winner will be returned through winnerch.
// If there is an error, the channel will be closed without sending a value.
//
func (e *ElectionSite) StartElection() <-chan string {

	// ballot in progress
	if !e.master.setBallotInProg(true) || e.IsClosed() {
		return nil
	}

	// create a buffered channel so sender won't block.
	winnerch := make(chan string, 1)

	go e.master.castBallot(winnerch)

	return (<-chan string)(winnerch)
}

//
// Close ElectionSite.  Any pending ballot will be closed immediately.
//
//
func (e *ElectionSite) Close() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if !e.isClosed {
		if common.Debug() {
			log.Printf("ElectionSite.Close() : Diagnostic Stack ...")
			log.Printf("%s", debug.Stack())
		}

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
// Update the winning epoch. The epoch can change after
// the synchronization phase (when leader tells the
// follower what is the actual epoch value -- after the
// leader gets a quorum of followers).  There are other
// possible implementations (e.g. keeping the winning
// vote with the server -- not the ballotMaster), but
// for now, let's just have this API to update the
// epoch. Note that this is just a public wrapper
// method on top of ballotMaster.
//
func (s *ElectionSite) UpdateWinningEpoch(epoch uint32) {

	s.master.updateWinningEpoch(epoch)
}

/////////////////////////////////////////////////////////////////////////////
// ElectionSite (Private)
/////////////////////////////////////////////////////////////////////////////

//
// Create Vote from State
//
func (s *ElectionSite) createVoteFromCurState() VoteMsg {

	epoch, err := s.handler.GetCurrentEpoch()
	if err != nil {
		// if epoch is missing, set the epoch to the smallest possible
		// number.  This is to allow the voting peers to tell me what
		// the right epoch would be during balloting.  This allows me
		// to proceed leader election.  After leader election, this
		// node will either be a leader or follower, and it will need
		// to synchornize with the peer's state (acceptedEpoch, currentEpoch).
		epoch = common.BOOTSTRAP_CURRENT_EPOCH
	}

	lastLoggedTxid, err := s.handler.GetLastLoggedTxid()
	if err != nil {
		// if txid is missing, set the txid to the smallest possible
		// number.  This likely will cause the peer to ignore my vote.
		lastLoggedTxid = common.BOOTSTRAP_LAST_LOGGED_TXID
	}

	lastCommittedTxid, err := s.handler.GetLastCommittedTxid()
	if err != nil {
		// if txid is missing, set the txid to the smallest possible
		// number.  This likely will cause the peer to ignore my vote.
		lastCommittedTxid = common.BOOTSTRAP_LAST_COMMITTED_TXID
	}

	vote := s.factory.CreateVote(s.master.round,
		uint32(s.handler.GetStatus()),
		epoch,
		s.messenger.GetLocalAddr(), // this is localhost UDP port
		uint64(lastLoggedTxid),
		uint64(lastCommittedTxid),
		s.solicitOnly)

	return vote
}

//
// Tell if a particular voter is in the ensemble
//
func (s *ElectionSite) inEnsemble(voter net.Addr) bool {
	for _, peer := range s.fullEnsemble {
		if peer == voter.String() {
			return true
		}
	}
	return false
}

//
// Create an ensemble for voting
//
func cloneEnsemble(peers []string, laddr string) ([]net.Addr, []string, error) {

	en := make([]net.Addr, 0, len(peers))
	fullEn := make([]string, 0, len(peers)+1)

	for i := 0; i < len(peers); i++ {
		rAddr, err := net.ResolveUDPAddr("udp", peers[i])
		if err != nil {
			return nil, nil, err
		}
		en = append(en, rAddr)
		fullEn = append(fullEn, rAddr.String())
	}

	fullEn = append(fullEn, laddr)

	return en, fullEn, nil
}

/////////////////////////////////////////////////////////////////////////////
// ballotMaster
/////////////////////////////////////////////////////////////////////////////

//
// Create a new ballotMaster.
//
func newBallotMaster(site *ElectionSite) *ballotMaster {

	master := &ballotMaster{site: site,
		winner: nil,
		round:  gElectionRound,
		inProg: false}

	return master
}

//
// Start a new round of ballot.
//
func (b *ballotMaster) castBallot(winnerch chan string) {

	// close the channel to make sure that the caller won't be
	// block forever.  If the balltot is successful, a value would
	// have sent to the channel before being closed. Otherwise,
	// a closed channel without value means the ballot is not
	// successful.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in ballotMaster.castBallot() : %s\n", r)
			common.SafeRun("ballotMaster.castBallot()",
				func() {
					b.site.Close()
				})
		}

		common.SafeRun("ballotMaster.castBallot()",
			func() {
				close(winnerch) // unblock caller

				// balloting complete
				b.setBallotInProg(false)
			})
	}()

	// create a channel to receive the ballot result
	// should only be closed by Poll Worker.  Make
	// if buffered so the sender won't block.
	resultch := make(chan bool, 1)

	// Create a new ballot
	ballot := b.createInitialBallot(resultch)

	// Tell the worker to observe this ballot.  This forces
	// the worker to start collecting new ballot result.
	b.site.worker.observe(ballot)

	// let the peer to know about this ballot.  It is expected
	// that the peer will reply with a vote.
	b.site.messenger.Multicast(ballot.result.proposed, b.site.ensemble)

	success, ok := <-resultch
	if !ok {
		// channel close. Ballot done
		success = false
	}

	// Announce the winner
	if success {
		winner, ok := b.GetWinner()
		if ok {
			common.SafeRun("ballotMaster.castBallot()",
				func() {
					// Remember the last round.
					gElectionRound = b.round
					// Announce the result
					winnerch <- winner
				})
		}
	}
}

//
// close the ballot master.
//
func (b *ballotMaster) close() {
	// Nothing to do now (jsut placeholder).   The current ballot
	// is closed when the ballot resultch is closed.
	// Instead of doing it in this method, should
	// do it in the poll worker to avoid race condition.
}

//
// Update the flag indicating if there is a balllot in progress. Return value
// is true if the flag value was changed.
//
func (b *ballotMaster) setBallotInProg(value bool) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.inProg == value {
		return false
	}

	b.inProg = value
	return true
}

//
// Get the next id for ballot.
//
func (b *ballotMaster) getNextRound() uint64 {

	result := b.round
	b.round++
	return result
}

//
// Create a ballot
//
func (b *ballotMaster) createInitialBallot(resultch chan bool) *Ballot {

	result := &ballotResult{winningEpoch: 0,
		receivedVotes: make(map[string]VoteMsg),
		activePeers:   make(map[string]VoteMsg)}

	ballot := &Ballot{result: result,
		resultch: resultch}

	b.getNextRound()
	newVote := b.site.createVoteFromCurState()
	ballot.updateProposed(newVote, b.site)

	return ballot
}

//
// Copy a winning vote.  This function is called when
// there is no active ballot going on.
//
func (b *ballotMaster) cloneWinningVote() VoteMsg {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If b.winner is not nil, then it indicates that I have concluded my leader
	// election.
	if b.winner != nil {
		return b.site.factory.CreateVote(
			b.winner.proposed.GetRound(),
			uint32(b.site.handler.GetStatus()),
			b.winner.winningEpoch,
			b.winner.proposed.GetCndId(),
			b.winner.proposed.GetCndLoggedTxnId(),
			b.winner.proposed.GetCndCommittedTxnId(),
			b.site.solicitOnly)
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Function for upkeeping the election state.  These covers function from
// ballotMaster and Ballot.
/////////////////////////////////////////////////////////////////////////////

//
// Return the winner
//
func (b *ballotMaster) GetWinner() (string, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.winner != nil {
		return b.winner.proposed.GetCndId(), true
	}

	return "", false
}

//
// Set the winner
//
func (b *ballotMaster) setWinner(result *ballotResult) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.winner = result
	b.winner.winningEpoch = result.proposed.GetEpoch()
}

//
// Update the epcoh of the winning vote
//
func (b *ballotMaster) updateWinningEpoch(epoch uint32) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.winner.winningEpoch = epoch
}

//
// Set the current round.  This function is there just for
// easier to keep track of different places that set
// the ballotMaster.round.   ballotMaster.round should
// always be in sycn with the ballot.result.proposed.round or
// the master.winner.proposed.round.
//
func (b *ballotMaster) setCurrentRound(round uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.round = round
}

//
// Get the current round
//
func (b *ballotMaster) getCurrentRound() uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.round
}

//
// Make the given vote as the proposed vote. Since pollWorker
// executes serially, this does not need mutex.
//
func (b *Ballot) updateProposed(proposed VoteMsg, site *ElectionSite) {

	// update the ballot
	b.result.proposed = proposed

	// esnure the ballotMaster's round matches the proposed vote.
	// These 2 values should be always in sync.
	site.master.setCurrentRound(proposed.GetRound())

	// update the recieved votes (for quorum)
	b.result.receivedVotes[site.messenger.GetLocalAddr()] = proposed
}

//
// Reset the ballot with a new proposed vote
//
func (b *Ballot) resetAndUpdateProposed(proposed VoteMsg, site *ElectionSite) {
	b.result.receivedVotes = make(map[string]VoteMsg)
	// To be safe, clean up the active peers as well.  This is just to ensure
	// when an active peers becomes an electing peer, we don't keep old votes
	// around.  This deviates from ZK (which does not clear the active votes
	// -- possibly for faster convergence to quorum).
	b.result.activePeers = make(map[string]VoteMsg)

	// update the proposed
	b.updateProposed(proposed, site)
}

/////////////////////////////////////////////////////////////////////////////
// pollWorker
/////////////////////////////////////////////////////////////////////////////

//
// Create a new pollWorker.  The pollWorker listens to the Vote receving from
// the peers for a particular ballot.
//
func startPollWorker(site *ElectionSite) *pollWorker {

	worker := &pollWorker{site: site,
		ballot:   nil,
		killch:   make(chan bool, 1),    // make sure sender won't block
		listench: make(chan *Ballot, 1)} // make sure sender won't block

	go worker.listen()

	return worker
}

//
// Notify the pollWorker that there is a new ballot.
//
func (w *pollWorker) observe(ballot *Ballot) {
	// This synchronous.  This is to ensure that listen() receives the ballot
	// before this function return to the ballotMaster.
	w.ballot = ballot
	w.listench <- ballot
}

//
// Close the pollWorker
//
func (p *pollWorker) close() {
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
func (w *pollWorker) listen() {

	// If this loop terminates (e.g. due to panic), then make sure
	// there is no outstanding ballot waiting for a result.   Close
	// any channel for outstanding ballot such that the caller
	// won't get blocked forever.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in pollWorker.listen() : %s\n", r)
		}

		// make sure we close the ElectionSite first such that
		// there is no new ballot coming while we are shutting
		// down the pollWorker. If not, then the some go-routine
		// may be waiting forever for the new ballot to complete.
		common.SafeRun("pollWorker.listen()",
			func() {
				w.site.Close()
			})

		// unlock anyone waiting for existing ballot to complete.
		common.SafeRun("pollWorker.listen()",
			func() {
				if w.ballot != nil {
					close(w.ballot.resultch)
					w.ballot = nil
				}
			})
	}()

	// Get the channel for receiving votes from the peer.
	reqch := w.site.messenger.DefaultReceiveChannel()

	timeout := common.NewBackoffTimer(
		common.BALLOT_TIMEOUT*time.Millisecond,
		common.BALLOT_MAX_TIMEOUT*time.Millisecond,
		2,
	)

	inFinalize := false
	finalizeTimer := common.NewStoppedResettableTimer(common.BALLOT_FINALIZE_WAIT * time.Millisecond)

	for {
		select {
		case w.ballot = <-w.listench: // listench should never close
			{
				// Before listening to any vote, see if we reach quorum already.
				// This should only happen if there is only one server in the
				// ensemble.  If this election is for solicit purpose, then
				// run election all the time.
				if !w.site.solicitOnly &&
					w.checkQuorum(w.ballot.result.receivedVotes, w.ballot.result.proposed) {
					w.site.master.setWinner(w.ballot.result)
					w.ballot.resultch <- true
					w.ballot = nil
				} else {
					// There is a new ballot.
					timeout.Reset()
					inFinalize = false
					finalizeTimer.Stop()
				}
			}
		// Receiving a vote
		case msg, ok := <-reqch:
			{
				if !ok {
					return
				}

				// Receive a new vote.  The voter is identified by its UDP port,
				// which must remain the same during the election phase.
				vote := msg.Content.(VoteMsg)
				voter := msg.Peer

				// If I am receiving a vote that just for soliciting my response,
				// then respond with my winning vote only after I am confirmed as
				// either a leader or follower.  This ensure that the watcher will
				// only find a leader from a stable ensemble.  This also ensures
				// that the watcher will only count the votes from active participant,
				// therefore, it will not count from other watcher as well as its
				// own vote (code path for handling votes from electing member will
				// never called for watcher).
				if vote.GetSolicit() {
					status := w.site.handler.GetStatus()
					if status == LEADING || status == FOLLOWING {
						w.respondInquiry(voter, vote)
					}
					continue
				}

				// Check if the voter is in the ensemble
				if !w.site.inEnsemble(voter) {
					continue
				}

				if w.ballot == nil {
					// If there is no ballot or the vote is from a watcher,
					// then just need to respond if I have a winner.
					w.respondInquiry(voter, vote)
					continue
				}

				timeout.Reset()

				proposed := w.cloneProposedVote()
				if w.handleVote(voter, vote) {
					proposedUpdated :=
						w.compareVote(w.ballot.result.proposed, proposed) != common.EQUAL

					if !inFinalize || proposedUpdated {
						inFinalize = true
						finalizeTimer.Reset()
					}
				} else {
					if inFinalize {
						// we had a quorum but not anymore
						inFinalize = false
						finalizeTimer.Stop()
					}
				}
			}
		case <-finalizeTimer.C:
			{
				// we achieve quorum, set the winner.
				// setting the winner and usetting the ballot
				// should be done together.
				// NOTE: ZK does not notify other peers when this node has
				// select a leader
				w.site.master.setWinner(w.ballot.result)
				w.ballot.resultch <- true
				w.ballot = nil
				timeout.Stop()
			}
		case <-timeout.GetChannel():
			{
				// If there is a timeout but no response, send vote again.
				if w.ballot != nil {
					w.site.messenger.Multicast(w.cloneProposedVote(), w.site.ensemble)
					timeout.Backoff()
				}
			}
		case <-w.killch:
			{
				return
			}
		}
	}
}

//
// The pollWorker is no longer in election.  Respond to inquiry from
// the peer.
//
func (w *pollWorker) respondInquiry(voter net.Addr, vote VoteMsg) {

	if PeerStatus(vote.GetStatus()) == ELECTING {
		// Make sure that we only send this when there is a winning vote, such
		// that the vote has a majority support.
		msg := w.site.master.cloneWinningVote()
		if msg != nil {
			// send the winning vote if there is no error
			w.site.messenger.Send(msg, voter)
		}
	}
}

//
// Handle a new vote.
//
func (w *pollWorker) handleVote(voter net.Addr, vote VoteMsg) bool {

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
func (w *pollWorker) handleVoteForElectingPeer(voter net.Addr, vote VoteMsg) bool {

	// compare the round.  When there are electing peers, they will eventually
	// converge to the same round when quorum is reached.  This implies that
	// an established ensemble should share the same round, and this value
	// remains stable for the ensemble.
	compareRound := w.compareRound(vote)

	// if the incoming vote has a greater round, re-ballot.
	if compareRound == common.GREATER {

		// update the current round.  This need to be done
		// before updateProposed() is called.
		w.site.master.setCurrentRound(vote.GetRound())

		if w.compareVoteWithCurState(vote) == common.GREATER {
			// Update my vote if the incoming vote is larger.
			w.ballot.resetAndUpdateProposed(vote, w.site)
		} else {
			// otherwise udpate my vote using lastLoggedTxid
			w.ballot.resetAndUpdateProposed(w.site.createVoteFromCurState(), w.site)
		}

		// notify that our new vote
		w.site.messenger.Multicast(w.cloneProposedVote(), w.site.ensemble)

		// if we reach quorum with this vote, announce the result
		// and stop election
		return w.acceptAndCheckQuorum(voter, vote)

	} else if compareRound == common.EQUAL {
		// if it is the same round and the incoming vote has higher epoch or txid,
		// update myself to the incoming vote and broadcast my new vote
		switch w.compareVoteWithProposed(vote) {
		case common.GREATER:
			// update and notify that our new vote
			w.ballot.updateProposed(vote, w.site)
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
		case common.EQUAL:
			return w.acceptAndCheckQuorum(voter, vote)
		}
	} else {
		// My round is higher. Send back the notification to the sender with my round
		w.site.messenger.Send(w.cloneProposedVote(), voter)
	}

	return false
}

//
// Handle a new vote from a leader or follower.   This implies that this vote
// has already reached quorum and this node belongs to the quorum.  When we
// reach this method, it can be:
// 1) An new ensemble is converging from a set of electing nodes.  So nodes are
//    reaching this conclusion faster than I am.
// 2) I am joining an established ensemble (I rejoin the network or restart).
// 3) A node from an established ensemble responds to me, but the ensemble
//    could soon be dissolve (lose majority) after the node sends the message.
// 4) An rogue node re-join the network while I am running election
//    (due to bug/race condition?).
//
func (w *pollWorker) handleVoteForActivePeer(voter net.Addr, vote VoteMsg) bool {

	// compare the round
	compareRound := w.compareRound(vote)

	if compareRound == common.EQUAL {
		// If I recieve a vote with the same round, then it could mean
		// that an esemble is forming from a set of electing peers.  Add
		// this vote to the list of received votes.  All the received votes
		// are from the same round.  If we get a quorum from the received
		// votes, then announce the result.
		// NOTE: ZK does not check the epoch nor update the proposed vote upon
		// receiving a vote from an active member (unlike receiving a vote from
		// electing peer).  This implies that if this is a rogue vote (a node
		// sends out a vote and the ensemble loses majority), the election alogrithm
		// will not get affected -- it can still converge if there is majority of
		// electing peer to reach quorum.  If the established ensmeble remains stable,
		// then there should be enough active member responds to me and I will
		// eventually reach quorum (based on ballot.result.activePeers -- see below).
		w.ballot.result.receivedVotes[voter.String()] = vote

		if w.checkQuorum(w.ballot.result.receivedVotes, vote) && w.certifyLeader(vote) {
			// accept this vote from the peer
			w.ballot.updateProposed(vote, w.site)
			return true
		}
	}

	// The active peer has chosen a leader, but we cannot confirm it yet.
	// Keep the active peer onto a different list, since receivedVotes
	// can be reset (all received votes must be from the same round).
	// If this peer goes down after sending us his vote, his vote still count
	// in this ballot.  By calling certifyLeader(), we can also makes sure that
	// the candidate has established itself to us as a leader.
	w.ballot.result.activePeers[voter.String()] = vote

	// Check the quorum only for the active peers.   In this case, the vote
	// can have a different round than mime.   There may already be an established
	// ensemble and I am merely trying to join them.
	if w.checkQuorum(w.ballot.result.activePeers, vote) && w.certifyLeader(vote) {

		w.ballot.updateProposed(vote, w.site)
		return true
	}

	return false
}

//
// Compare the current round with the given vote
//
func (w *pollWorker) compareRound(vote VoteMsg) common.CompareResult {

	currentRound := w.site.master.round

	if vote.GetRound() == currentRound {
		return common.EQUAL
	}

	if vote.GetRound() > currentRound {
		return common.GREATER
	}

	return common.LESSER
}

//
// Compare two votes.  Return true if vote1 is larger than vote2.
//
func (w *pollWorker) compareVote(vote1, vote2 VoteMsg) common.CompareResult {

	// Vote with the larger epoch always is larger
	result := common.CompareEpoch(vote1.GetEpoch(), vote2.GetEpoch())

	if result == common.MORE_RECENT {
		return common.GREATER
	}

	if result == common.LESS_RECENT {
		return common.LESSER
	}

	// If a candidate has a larger logged txid, it means the candidate
	// has processed more proposals.   This vote is larger.
	if vote1.GetCndLoggedTxnId() > vote2.GetCndLoggedTxnId() {
		return common.GREATER
	}

	if vote1.GetCndLoggedTxnId() < vote2.GetCndLoggedTxnId() {
		return common.LESSER
	}

	// This candidate has the same number of proposals in his committed log as
	// the other one. But if a candidate has a larger committed txid,
	// it means this candidate also has processed more commit messages from the
	// previous leader.   This vote is larger.
	if vote1.GetCndCommittedTxnId() > vote2.GetCndCommittedTxnId() {
		return common.GREATER
	}

	if vote1.GetCndCommittedTxnId() < vote2.GetCndCommittedTxnId() {
		return common.LESSER
	}

	// All else is equal (e.g. during inital system startup -- repository is emtpy),
	// use the ip address.
	if vote1.GetCndId() > vote2.GetCndId() {
		return common.GREATER
	}

	if vote1.GetCndId() < vote2.GetCndId() {
		return common.LESSER
	}

	return common.EQUAL
}

//
// Compare the given vote with currennt state (epoch, lastLoggedTxnid)
//
func (w *pollWorker) compareVoteWithCurState(vote VoteMsg) common.CompareResult {

	vote2 := w.site.createVoteFromCurState()
	return w.compareVote(vote, vote2)
}

//
// Compare the given vote with proposed vote
//
func (w *pollWorker) compareVoteWithProposed(vote VoteMsg) common.CompareResult {

	return w.compareVote(vote, w.ballot.result.proposed)
}

//
// Accept the check quorum
//
func (w *pollWorker) acceptAndCheckQuorum(voter net.Addr, vote VoteMsg) bool {

	// Remember this peer's vote.  Note that ZK never takes away a voter's votes
	// even if the voter has gone down (ZK would not know).  But ZK will ensure
	// that the new leader will have a quorum of followers (in synchronization/recovery
	// phase) before the ensemble become stable.
	w.ballot.result.receivedVotes[voter.String()] = vote
	return w.checkQuorum(w.ballot.result.receivedVotes, w.ballot.result.proposed)

	//TODO: After quorum is reached, ZK will wait to see if there is any additional
	//messages that will change leader.  It is possibly an optimization because if
	//it is a wrong leader, it will fail during discovery/sync and force a new
	//re-election.
}

//
// Check Quorum
//
func (w *pollWorker) checkQuorum(votes map[string]VoteMsg, candidate VoteMsg) bool {

	count := 0
	for _, vote := range votes {
		if PeerStatus(vote.GetStatus()) == ELECTING ||
			PeerStatus(candidate.GetStatus()) == ELECTING {
			if w.compareVote(vote, candidate) == common.EQUAL &&
				vote.GetRound() == candidate.GetRound() {
				count++
			}
		} else if vote.GetCndId() == candidate.GetCndId() &&
			vote.GetEpoch() == candidate.GetEpoch() {
			count++
		}
	}

	return w.site.handler.GetQuorumVerifier().HasQuorum(count)
}

//
// Copy a proposed vote
//
func (w *pollWorker) cloneProposedVote() VoteMsg {

	// w.site.master.round should be in sycn with
	// w.ballot.result.proposed.round. Use w.site.master.round
	// to be consistent.
	return w.site.factory.CreateVote(w.site.master.round,
		uint32(w.site.handler.GetStatus()),
		uint32(w.ballot.result.proposed.GetEpoch()),
		w.ballot.result.proposed.GetCndId(),
		w.ballot.result.proposed.GetCndLoggedTxnId(),
		w.ballot.result.proposed.GetCndCommittedTxnId(),
		w.site.solicitOnly)
}

//
// Certify the leader before declaring followship
//
func (w *pollWorker) certifyLeader(vote VoteMsg) bool {

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

	messenger, err := common.NewPeerMessenger(laddr, nil)
	if err != nil {
		return nil, err
	}

	return messenger, nil
}
