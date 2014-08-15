package protocol

import (
	"common"
	"sync"
	"log"
	"fmt"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type LeaderSyncProxy struct {
	state         *ConsentState
	follower      *common.PeerPipe
	handler        ActionHandler
	factory        MsgFactory
	followerState *FollowerState
	
	mutex          sync.Mutex
	isClosed       bool
	donech         chan bool  
	killch         chan bool
}

type FollowerSyncProxy struct {
	leader  *common.PeerPipe
	handler  ActionHandler
	factory  MsgFactory
	state   *FollowerState
	
	mutex          sync.Mutex
	isClosed       bool
	donech         chan bool
	killch         chan bool
}

type ConsentState struct {
	acceptedEpoch       uint32
	acceptedEpochSet    map[string]uint32
	acceptedEpochCond  *sync.Cond
	acceptedEpochMutex  sync.Mutex

	ackEpochSet   		 map[string]string
	ackEpochCond  		*sync.Cond
	ackEpochMutex 		 sync.Mutex

	newLeaderAckSet   	 map[string]string
	newLeaderAckCond  	*sync.Cond
	newLeaderAckMutex  	 sync.Mutex

	ensembleSize 		 uint64
}

type FollowerState struct {
	currentEpoch   uint32
	lastLoggedTxid uint64
	fid 		   string
}

type LeaderStageCode uint16

const (
	UPDATE_ACCEPTED_EPOCH_AFTER_QUORUM LeaderStageCode = iota
	NOTIFY_NEW_EPOCH
	UPDATE_CURRENT_EPOCH_AFTER_QUORUM
	SYNC_SEND
	DECLARE_NEW_LEADER_AFTER_QUORUM
	LEADER_SYNC_DONE
)

type FollowerStageCode uint16

const (
	SEND_FOLLOWERINFO FollowerStageCode = iota
	RECEIVE_UPDATE_ACCEPTED_EPOCH
	SYNC_RECEIVE
	RECEIVE_UPDATE_CURRENT_EPOCH
	FOLLOWER_SYNC_DONE
)

/////////////////////////////////////////////////////////////////////////////
// ConsentState
/////////////////////////////////////////////////////////////////////////////

//
// Create a new ConsentState for synchronization.   The leader must proceed in 
// 4 stages:
// 1) Reach quourm of followers for sending its persisted acceptedEpoch to the leader.
//    The leader uses followers acceptedEpoch to determine the next epoch.
// 2) Reach quorum of followers to accept the new epoch.   
// 3) Synchronizes the commit log between leader and each follower 
// 4) Reach quorum of followers to accepts this leader (NewLeaderAck)
//
// The ConsentState is used for keep that state for stages (1), (2) and (4) where
// quorum is required to proceed to next stage.
//
// The ConsentState is using the physical host (actual port) which is different for each TCP connection.  This requires
// the ConsentState to be cleaned up if synchronization with a particular follower aborts.   After synchronization
// with a follower succeeds, the follower's vote will stay in the ConsentState, since the main purpose of the 
// ConsentState is for voting on a new epoch, as well as establishing that a majority of followers are going
// to follower the leader.   A node can only establish leadership until stage 4 passes.  Once leadership is
// established, if the node looses majority of followers, the server should abort and go through re-election again 
// with a new ConsentState.
//
func NewConsentState(sid string, epoch uint32, ensemble uint64) *ConsentState {

	epoch = common.CompareAndIncrementEpoch(epoch, 0) // increment epoch to next value
	
	state := &ConsentState{acceptedEpoch: epoch,
		acceptedEpochSet: make(map[string]uint32),
		ackEpochSet:      make(map[string]string),
		newLeaderAckSet:  make(map[string]string),
		ensembleSize:     ensemble}

	state.acceptedEpochCond = sync.NewCond(&state.acceptedEpochMutex)
	state.ackEpochCond = sync.NewCond(&state.ackEpochMutex)
	state.newLeaderAckCond = sync.NewCond(&state.newLeaderAckMutex)

	// add the leader to both sets, since enemble size can count the leader as well.
	state.acceptedEpochSet[sid] = epoch
	state.ackEpochSet[sid] = sid
	state.newLeaderAckSet[sid] = sid

	return state
}

func (s *ConsentState) voteAcceptedEpoch(voter string, newEpoch uint32) (uint32, bool) {
	s.acceptedEpochCond.L.Lock()
	defer s.acceptedEpochCond.L.Unlock()

	// Reach quorum. Just Return
	if len(s.acceptedEpochSet) > int(s.ensembleSize/2) {
		return s.acceptedEpoch, true
	}

	s.acceptedEpochSet[voter] = newEpoch

	// This function can panic if we exceed epoch limit	
	s.acceptedEpoch = common.CompareAndIncrementEpoch(newEpoch, s.acceptedEpoch)

	if len(s.acceptedEpochSet) > int(s.ensembleSize/2) {
		// reach quorum. Notify
		s.acceptedEpochCond.Broadcast()
		return s.acceptedEpoch, true
	}

	// wait for quorum to be reached.  It is possible
	// that the go-routine is woken up before quorum is
	// reached (if Terminate() is called).   It is
	// also possible that a concurrent go-routine has
	// remove the voter after reaching quorum.  In these
	// cases, return false. 
	s.acceptedEpochCond.Wait()
	return s.acceptedEpoch, len(s.acceptedEpochSet) > int(s.ensembleSize/2)
}

func (s *ConsentState) removeAcceptedEpoch(voter string) {
	s.acceptedEpochCond.L.Lock()
	defer s.acceptedEpochCond.L.Unlock()
	
	delete(s.acceptedEpochSet, voter)
}

func (s *ConsentState) voteEpochAck(voter string) (bool) {
	s.ackEpochCond.L.Lock()
	defer s.ackEpochCond.L.Unlock()

	// Reach quorum. Just Return
	if len(s.ackEpochSet) > int(s.ensembleSize/2) {
		return true
	}

	s.ackEpochSet[voter] = voter

	if len(s.ackEpochSet) > int(s.ensembleSize/2) {
		// reach quorum. Notify
		s.ackEpochCond.Broadcast()
		return true
	}

	// wait for quorum to be reached.  It is possible
	// that the go-routine is woken up before quorum is
	// reached (if Terminate() is called).   It is
	// also possible that a concurrent go-routine has
	// remove the voter after reaching quorum.  In these
	// cases, return false. 
	s.ackEpochCond.Wait()
	return len(s.ackEpochSet) > int(s.ensembleSize/2)
}

func (s *ConsentState) removeEpochAck(voter string) {
	s.ackEpochCond.L.Lock()
	defer s.ackEpochCond.L.Unlock()
	
	delete(s.ackEpochSet, voter)
}

func (s *ConsentState) voteNewLeaderAck(voter string) (bool) {
	s.newLeaderAckCond.L.Lock()
	defer s.newLeaderAckCond.L.Unlock()

	// Reach quorum. Just Return
	if len(s.newLeaderAckSet) > int(s.ensembleSize/2) {
		return true
	}

	s.newLeaderAckSet[voter] = voter

	if len(s.newLeaderAckSet) > int(s.ensembleSize/2) {
		// reach quorum. Notify
		s.newLeaderAckCond.Broadcast()
		return true
	}

	// wait for quorum to be reached.  It is possible
	// that the go-routine is woken up before quorum is
	// reached (if Terminate() is called).   It is
	// also possible that a concurrent go-routine has
	// remove the voter after reaching quorum.  In these
	// cases, return false. 
	s.newLeaderAckCond.Wait()
	return len(s.newLeaderAckSet) > int(s.ensembleSize/2)
}

func (s *ConsentState) removeNewLeaderAck(voter string) {
	s.newLeaderAckCond.L.Lock()
	defer s.newLeaderAckCond.L.Unlock()
	
	delete(s.newLeaderAckSet, voter)
}

func (s *ConsentState) Terminate() {
	s.acceptedEpochCond.L.Lock()
	s.acceptedEpochCond.Broadcast()
	s.acceptedEpochCond.L.Unlock()
	
	s.ackEpochCond.L.Lock()
	s.ackEpochCond.Broadcast()
	s.ackEpochCond.L.Unlock()
	
	s.newLeaderAckCond.L.Lock()
	s.newLeaderAckCond.Broadcast()
	s.newLeaderAckCond.L.Unlock()
}

/////////////////////////////////////////////////////////////////////////////
// LeaderSyncProxy - Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a LeaderSyncProxy to synchronize with a follower.  The proxy 
// requires 2 stateful variables to be provided as inputs:
// 1) ConsentState:  The LeaderSyncProxy requires a quorum of followers 
//    to follow before leader can process client request.  The consentState
//    is a shared state (shared among multiple LeaderSyncProxy) to keep track of
//    the followers following this leader during synchronziation. Note
//    that a follower may leave the leader after synchronziation, but the
//    ConsentState will not keep track of follower leaving.
// 2) Follower: The follower is a PeerPipe (TCP connection).    This is
//    used to exchange messages with the follower node.
//
func NewLeaderSyncProxy(state *ConsentState,
	follower *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory) *LeaderSyncProxy {

	sync := &LeaderSyncProxy{state: state,
		follower: follower,
		handler:  handler,
		factory:  factory,
		donech : make(chan bool, 1), // donech should not be closed
		killch : make(chan bool, 1), // donech should not be closed
		isClosed: false}

	return sync
}

//
// Start synchronization with a speicfic follower.   This function
// can be run as regular function or go-routine.   If the caller runs this 
// as a go-routine, the caller should use GetDoneChannel()
// to tell when this function completes.
//
// There are 3 cases when this function sends "false" to donech:
// 1) When there is an error during synchronization
// 2) When synchronization timeout
// 3) Terminate() is called 
//
// When a failure (false) result is sent to the donech, this function
// will also close the PeerPipe to the follower.  This will force
// the follower to restart election.
//
// This function will catch panic.
//
func (l *LeaderSyncProxy) Start() bool {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in LeaderSyncProxy.Start() : %s\n", r)
			l.abort()  // ensure proper cleanup and unblock caller
		} 
		
		l.close()
	}()

	timeout := time.After(common.SYNC_TIMEOUT * time.Millisecond)

	// spawn a go-routine to perform synchronziation.  Do not close donech2, just
	// let it garbage collect when the go-routine is done.	 Make sure using
	// buffered channel since this go-routine may go away before execute() does.
	donech2 := make(chan bool, 1)
	go l.execute(donech2)
	
	select {
		case success, ok := <- donech2:
			if !ok {
				// channel is not going to close but just to be safe ... 
				success = false
			}
			l.donech <- success
			return success
		case <- timeout:
			log.Printf("LeaderSyncProxy.Start(): Synchronization timeout for peer (TCP %s). Terminate.", l.follower.GetAddr())
			l.abort()
		case <- l.killch:
			log.Printf("LeaderSyncProxy.Start(): Receive kill signal for peer (TCP %s).  Terminate.", l.follower.GetAddr())
			l.abort()
	}
	
	return false
}

//
// Terminate the syncrhonization with this follower.  Upon temrination, the follower
// will enter into election again.    This function cannot guarantee that the go-routine
// will terminate until the given ConsentState is terminated as well. 
// This function is an no-op if the LeaderSyncProxy already completes successfully.
//
func (l *LeaderSyncProxy) Terminate() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	if !l.isClosed {
		l.isClosed = true
		l.killch <- true
	}
}

//
// Return a channel that tells when the syncrhonization is done.  
// This is unbuffered channel such that LeaderSyncProxy will not be blocked
// upon completion of synchronization (whether successful or not).
//
func (l *LeaderSyncProxy) GetDoneChannel() <-chan bool {
	// do not return nil (can cause caller block forever)
	return (<-chan bool)(l.donech)
} 

//
// Return the fid (follower id) 
//
func (l *LeaderSyncProxy) GetFid() string {
	return l.followerState.fid
}

/////////////////////////////////////////////////////////////////////////////
// LeaderSyncProxy - Private Function
/////////////////////////////////////////////////////////////////////////////

//
// Abort the LeaderSyncProxy. 
//
func (l *LeaderSyncProxy) abort() {

	voter := l.GetFid()

	common.SafeRun("LeaderSyncProxy.abort()",
		func() {
			// terminate any on-going messaging with follower.  This will force
			// the follower to go through election again
			l.follower.Close()
		})

	common.SafeRun("LeaderSyncProxy.abort()",
		func() {
			// clean up the ConsentState
            l.state.removeAcceptedEpoch(voter) 
            l.state.removeEpochAck(voter) 
            l.state.removeNewLeaderAck(voter) 
		})
		
	// donech should never be closed.  But just to be safe ...
	common.SafeRun("LeaderSyncProxy.abort()",
		func() {
			l.donech <- false 
		})
}

//
// close the proxy
//
func (l *LeaderSyncProxy) close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	l.isClosed = true
}

//
// Main go-routine for handling sycnrhonization with a follower.  Note that
// this go-routine may be blocked by a non-interuptable condition variable, 
// in which the caller may have aborted.  donech must be a buffered channel
// to ensure that this go-routine will not get blocked if the caller dies first.
//
func (l *LeaderSyncProxy) execute(donech chan bool) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in LeaderSyncProxy.execute() : %s\n", r)
			donech <- false // unblock caller
		}
	}()
	
	var stage LeaderStageCode = UPDATE_ACCEPTED_EPOCH_AFTER_QUORUM

	for stage != LEADER_SYNC_DONE {
		switch stage {
		case UPDATE_ACCEPTED_EPOCH_AFTER_QUORUM:
			{
				err := l.updateAcceptedEpochAfterQuorum()
				if err != nil {
					log.Printf("LeaderSyncProxy.updateAcceptEpochAfterQuorum(): Error encountered = %s", err.Error())
					safeSend("LeaderSyncProxy:execute()", donech, false)
					return
				}
				stage = NOTIFY_NEW_EPOCH
			}
		case NOTIFY_NEW_EPOCH:
			{
				err := l.notifyNewEpoch() 
				if err != nil {
					log.Printf("LeaderSyncProxy.notifyNewEpoch(): Error encountered = %s", err.Error())
					safeSend("LeaderSyncProxy:execute()", donech, false)
					return
				}
				stage = UPDATE_CURRENT_EPOCH_AFTER_QUORUM
			}
		case UPDATE_CURRENT_EPOCH_AFTER_QUORUM:
			{
				err := l.updateCurrentEpochAfterQuorum() 
				if err != nil {
					log.Printf("LeaderSyncProxy.updateCurrentEpochAfterQuorum(): Error encountered = %s", err.Error())
					safeSend("LeaderSyncProxy:execute()", donech, false)
					return
				}
				stage = SYNC_SEND
			}
		case SYNC_SEND:
			{
				err := l.syncWithLeader() 
				if err != nil {
					log.Printf("LeaderSyncProxy.syncWithLeader(): Error encountered = %s", err.Error())
					safeSend("LeaderSyncProxy:execute()", donech, false)
					return
				}
				stage = DECLARE_NEW_LEADER_AFTER_QUORUM
			}
		case DECLARE_NEW_LEADER_AFTER_QUORUM:
			{
				err := l.declareNewLeaderAfterQuorum() 
				if err != nil {
					log.Printf("LeaderSyncProxy.declareNewLeaderAfterQuorum(): Error encountered = %s", err.Error())
					safeSend("LeaderSyncProxy:execute()", donech, false)
					return
				}
				stage = LEADER_SYNC_DONE
			}
		}
	}

	// Use SafeReturn just to be sure, even though donech should not be closed 
	safeSend("LeaderSyncProxy:execute()", donech, true)
}

func (l *LeaderSyncProxy) updateAcceptedEpochAfterQuorum() error {

	log.Printf("LeaderSyncProxy.updateAcceptedEpochAfterQuroum()")

	// Get my follower's vote for the accepted epoch
	packet, err := listen("FollowerInfo", l.follower)
	if err != nil {
		return err
	}

	// Get epoch from follower message
	info := packet.(FollowerInfoMsg)
	epoch := info.GetAcceptedEpoch()
	fid := info.GetFid()

	// initialize the follower state
	l.followerState = &FollowerState{lastLoggedTxid: 0, currentEpoch: 0, fid : fid}
	
	// update my vote and wait for epoch to reach quorum
	newEpoch, ok := l.state.voteAcceptedEpoch(l.GetFid(), epoch)
	if !ok {
		return common.NewError(common.ELECTION_ERROR, 
			"LeaderSyncProxy.updateAcceptedEpochAfterQuorum(): Fail to reach quorum on accepted epoch (FollowerInfo)")
	}

	// update the accepted epoch based on the quorum result.   This function
	// will perform update only if the new epoch is larger than existing value.
	l.handler.NotifyNewAcceptedEpoch(newEpoch)

	return nil
}

func (l *LeaderSyncProxy) notifyNewEpoch() error {

	log.Printf("LeaderSyncProxy.notifyNewEpoch()")
	
	epoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	packet := l.factory.CreateLeaderInfo(epoch)
	return send(packet, l.follower)
}

func (l *LeaderSyncProxy) updateCurrentEpochAfterQuorum() error {

	log.Printf("LeaderSyncProxy.updateCurrentEpochAfterQuorum()")
	
	// Get my follower's vote for the epoch ack
	packet, err := listen("EpochAck", l.follower)
	if err != nil {
		return err
	}

	// Get epoch from follower message
	// TODO : Validate follower epoch
	info := packet.(EpochAckMsg)
	l.followerState.currentEpoch = info.GetCurrentEpoch()
	l.followerState.lastLoggedTxid = info.GetLastLoggedTxid()

	// update my vote and wait for quorum of ack from followers
	ok := l.state.voteEpochAck(l.GetFid())
	if !ok {
		return common.NewError(common.ELECTION_ERROR, 
			"LeaderSyncProxy.updateCurrentEpochAfterQuorum(): Fail to reach quorum on current epoch (EpochAck)")
	}

	// update the current epock after quorum of followers have ack'ed
	epoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	
	// update the current epoch based on the quorum result.   This function
	// will perform update only if the new epoch is larger than existing value.
	l.handler.NotifyNewCurrentEpoch(epoch)

	return nil
}

func (l *LeaderSyncProxy) declareNewLeaderAfterQuorum() error {

	log.Printf("LeaderSyncProxy.declareNewLeaderAfterQuorum()")
	
	// return the new epoch to the follower
	epoch, err := l.handler.GetCurrentEpoch()
	if err != nil {
		return err
	}
	packet := l.factory.CreateNewLeader(epoch)
	err = send(packet, l.follower)
	if err != nil {
		return err
	}

	// Get the new leader ack
	ack, err := listen("NewLeaderAck", l.follower)
	if err != nil {
		return err
	}

	// TODO : Verify the ack
	ack = ack // TODO : just to get around compile error

	// update my vote and wait for quorum of ack from followers
	ok := l.state.voteNewLeaderAck(l.GetFid())
	if !ok {
		return common.NewError(common.ELECTION_ERROR, 
			"LeaderSyncProxy.declareNewLeaderAfterQuorum(): Fail to reach quorum on NewLeaderAck")
	}

	return nil
}

func (l *LeaderSyncProxy) syncWithLeader() error {

	log.Printf("LeaderSyncProxy.syncWithLeader()")
	
	logChan, errChan, err := l.handler.GetCommitedEntries(l.followerState.lastLoggedTxid)
	if err != nil {
		return err
	}

	for {
		select {
		case entry, ok := <-logChan:
			if !ok {
				// channel close, nothing to send
				return nil
			}

			err = send(entry, l.follower)
			if err != nil {
				return err
			}

		case err := <-errChan:
			if err != nil {
				return err
			}
			return nil
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// FollowerSyncProxy - Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a FollowerSyncProxy to synchronize with a leader.  The proxy 
// requires 1 stateful variables to be provided as inputs:
// 1) Leader : The leader is a PeerPipe (TCP connection).    This is
//    used to exchange messages with the leader node.
//
func NewFollowerSyncProxy(leader *common.PeerPipe,
	handler ActionHandler,
	factory MsgFactory) *FollowerSyncProxy {

	sync := &FollowerSyncProxy{leader: leader,
		handler: handler,
		factory: factory,
		state:   nil,
		donech : make(chan bool, 1), // donech should not be closed
		killch : make(chan bool, 1), // donech should not be closed
		isClosed: false}

	return sync
}

//
// Start synchronization with a speicfic leader.   This function
// can be run as regular function or go-routine.   If the caller runs this 
// as a go-routine, the caller should use GetDoneChannel()
// to tell when this function completes.
//
// There are 3 cases when this function sends "false" to donech:
// 1) When there is an error during synchronization
// 2) When synchronization timeout
// 3) Terminate() is called 
//
// When a failure (false) result is sent to the donech, this function
// will also close the PeerPipe to the leader.  This will force
// the leader to skip this follower. 
//
// This function will catch panic.
//
func (f *FollowerSyncProxy) Start() bool {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in FollowerSyncProxy.Start() : %s\n", r)
			f.abort()  // ensure proper cleanup and unblock caller
		} 
		
		f.close()
	}()
	
	timeout := time.After(common.SYNC_TIMEOUT * time.Millisecond)
	
	// spawn a go-routine to perform synchronziation.  Do not close donech2, just
	// let it garbage collect when the go-routine is done.	 Make sure using
	// buffered channel since this go-routine may go away before execute() does.
	donech2 := make(chan bool, 1) 
	go f.execute(donech2)
	
	select {
		case success, ok := <- donech2:
			if !ok {
				success = false
			}
			f.donech <- success
			return success
		case <- timeout:
			log.Printf("FollowerSyncProxy.Start(): Synchronization timeout for peer %s. Terminate.", f.leader.GetAddr())
			f.abort()
		case <- f.killch:
			log.Printf("FollowerSyncProxy.Start(): Receive kill signal for peer %s.  Terminate.", f.leader.GetAddr())
			f.abort()
	}
	
	return false
}

//
// Terminate the syncrhonization with this leader.  Upon temrination, the leader 
// will skip this follower.  This function is an no-op if the FollowerSyncProxy 
// already completes successfully.
//
func (l *FollowerSyncProxy) Terminate() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	if !l.isClosed {
		l.isClosed = true
		l.killch <- true
	}
}

//
// Return a channel that tells when the syncrhonization is done.  
// This is unbuffered channel such that FollowerSyncProxy will not be blocked
// upon completion of synchronization (whether successful or not).
//
func (l *FollowerSyncProxy) GetDoneChannel() <-chan bool {
	// do not return nil (can cause caller block forever)
	return (<-chan bool)(l.donech)
} 

/////////////////////////////////////////////////////////////////////////////
// FollowerSyncProxy - Private Function
/////////////////////////////////////////////////////////////////////////////

//
// Abort the FollowerSyncProxy.  By killing the leader's PeerPipe,
// the execution go-rountine will eventually error out and terminate by itself.
//
func (f *FollowerSyncProxy) abort() {

	common.SafeRun("FollowerSyncProxy.abort()",
		func() {
			// terminate any on-going messaging with follower 
			f.leader.Close()
		})
		
	common.SafeRun("FollowerSyncProxy.abort()",
		func() {
			f.donech <- false
		})
}

//
// close the proxy
//
func (l *FollowerSyncProxy) close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	l.isClosed = true
}

func (l *FollowerSyncProxy) execute(donech chan bool) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in FollowerSyncProxy.execute() : %s\n", r)
			donech <- false // unblock caller
		}
	}()
	
	var stage FollowerStageCode = SEND_FOLLOWERINFO

	for stage != FOLLOWER_SYNC_DONE {
		switch stage {
		case SEND_FOLLOWERINFO:
			{
				err := l.sendFollowerInfo() 
				if err != nil {
					log.Printf("FollowerSyncProxy.sendFollowerInfo(): Error encountered = %s", err.Error())
					safeSend("FollowerSyncProxy:execute()", donech, false)
					return
				}
				stage = RECEIVE_UPDATE_ACCEPTED_EPOCH
			}
		case RECEIVE_UPDATE_ACCEPTED_EPOCH:
			{
				err := l.receiveAndUpdateAcceptedEpoch() 
				if err != nil {
					log.Printf("FollowerSyncProxy.receiveAndUpdateAcceptedEpoch(): Error encountered = %s", err.Error())
					safeSend("FollowerSyncProxy:execute()", donech, false)
					return
				}
				stage = SYNC_RECEIVE
			}
		case SYNC_RECEIVE:
			{
				err := l.syncReceive() 
				if err != nil {
					log.Printf("FollowerSyncProxy.syncReceive(): Error encountered = %s", err.Error())
					safeSend("FollowerSyncProxy:execute()", donech, false)
					return
				}
				stage = RECEIVE_UPDATE_CURRENT_EPOCH
			}
		case RECEIVE_UPDATE_CURRENT_EPOCH:
			{
				err := l.receiveAndUpdateCurrentEpoch() 
				if err != nil {
					log.Printf("FollowerSyncProxy.receiveAndUpdateCurrentEpoch(): Error encountered = %s", err.Error())
					safeSend("FollowerSyncProxy:execute()", donech, false)
					return
				}
				stage = FOLLOWER_SYNC_DONE
			}
		}
	}

	safeSend("FollowerSyncProxy:execute()", donech, true)
}

func (l *FollowerSyncProxy) sendFollowerInfo() error {

	log.Printf("LeaderSyncProxy.sendFollowerInfo()")
	
	// Send my accepted epoch to the leader for voting (don't send current epoch)
	epoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	packet := l.factory.CreateFollowerInfo(epoch, l.handler.GetFollowerId())
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) receiveAndUpdateAcceptedEpoch() error {

	log.Printf("LeaderSyncProxy.receiveAndUpdateAcceptedEpoch()")
	
	// Get the accepted epoch from the leader.   This epoch
	// is already being voted on by multiple followers (the highest
	// epoch among the quorum of followers).
	packet, err := listen("LeaderInfo", l.leader)
	if err != nil {
		return err
	}

	// Get epoch from leader message
	info := packet.(LeaderInfoMsg)
	epoch := info.GetAcceptedEpoch()
	if err != nil {
		return err
	}

	acceptedEpoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	if epoch > acceptedEpoch {
		// Update the accepted epoch based on the quorum result.   This function
		// will perform update only if the new epoch is larger than existing value.
		// Once the accepted epoch is updated, it will not be reset even if the
		// sychornization with the leader fails.  Therefore, the follower will always
		// remember the largest accepted epoch known to it, such that it can be used
		// in the next round of voting.   Note that the leader derives this new accepted
		// epoch only after it has polled from a quorum of followers.  So even if sync fails,
		// it is unlikey that in the next sync, the leader will give a new accepted epoch smaller
		// than what is being stored now. 
		l.handler.NotifyNewAcceptedEpoch(epoch)
	} else if epoch == acceptedEpoch {
		// In ZK, if the local epoch (acceptedEpoch) == leader's epoch (epoch), it will replly an EpochAck with epoch = -1.  
		// This is to tell the leader that it should not count this EpockAck when computing quorum of EpochAck. 
		// This is to ensure that this follower does not "double ack" to the leader (e.g. when this follower rejoins a
		// stable ensemble).   In our implementation for ConsentState, it should not be affected by double ack from the same host. 
	} else {
		return common.NewError(common.PROTOCOL_ERROR, "Accepted Epoch from leader is smaller or equal to my epoch.")
	}

	// Notify the leader that I have accepted the epoch.  Send
	// the last logged txid and current epoch to the leader.
	txid, err := l.handler.GetLastLoggedTxid()
	if err != nil {
		return err
	}
	currentEpoch, err := l.handler.GetCurrentEpoch()
	if err != nil {
		return err
	}
	l.state = &FollowerState{lastLoggedTxid: uint64(txid), currentEpoch: currentEpoch}
	packet = l.factory.CreateEpochAck(uint64(txid), currentEpoch)
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) receiveAndUpdateCurrentEpoch() error {

	log.Printf("LeaderSyncProxy.receiveAndUpdateCurrentEpoch()")
	
	// Get the accepted epoch from the leader.   This epoch
	// is already being voted on by multiple followers (the highest
	// epoch among the quorum of followers).
	packet, err := listen("NewLeader", l.leader)
	if err != nil {
		return err
	}

	// Get epoch from follower message
	info := packet.(NewLeaderMsg)
	epoch := info.GetCurrentEpoch()

	// TODO : validate the epoch from leader

	// Update the current epoch based on the quorum result.   This function
	// will perform update only if the new epoch is larger than existing value.
	// Once the current epoch is updated, it will not be reset even if the
	// sychornization with the leader fails.  Therefore, the follower will always
	// remember the largest current epoch known to it, such that it can be used
	// in the next round of voting.   Note that the leader derives this new current 
	// epoch only after it has polled from a quorum of followers.  So even if sync fails,
	// it is unlikey that in the next sync, the leader will give a new current epoch smaller
	// than what is being stored now. 
	l.handler.NotifyNewCurrentEpoch(epoch)

	// Notify the leader that I have accepted the epoch
	packet = l.factory.CreateNewLeaderAck()
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) syncReceive() error {

	log.Printf("LeaderSyncProxy.syncReceive()")
	
	for {
		packet, err := listen("LogEntry", l.leader)
		if err != nil {
			return err
		}

		entry := packet.(LogEntryMsg)
		lastTxnid := entry.GetTxnid()

		// If this is the first one, skip
		if entry.GetOpCode() == uint32(common.OPCODE_STREAM_BEGIN_MARKER) {
			log.Printf("LeaderSyncProxy.syncReceive(). Receive stream_begin.  Txid : %d", lastTxnid)
			
			// If it is the first entry, we expect the entry txid to be the same as my last logged txid
			if lastTxnid != l.state.lastLoggedTxid {
				return common.NewError(common.PROTOCOL_ERROR, 
					fmt.Sprintf("Expect to receive first LogEntryMsg with txnid = %d. Get %d", l.state.lastLoggedTxid, lastTxnid))
			}
			
			continue
		}
		
		// If this is the last one, then set the CommittedTxnid 
		if entry.GetOpCode() == uint32(common.OPCODE_STREAM_END_MARKER) {
			log.Printf("LeaderSyncProxy.syncReceive(). Receive stream_end.  Txid : %d", lastTxnid)
			l.handler.NotifyNewLastCommittedTxid(lastTxnid)		
			return nil
		}
	
		// write the new commit entry
		err = l.handler.AppendLog(entry.GetTxnid(), entry.GetOpCode(), entry.GetKey(), entry.GetContent())
		if err != nil {
			return err
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func listen(name string, pipe *common.PeerPipe) (common.Packet, error) {

	reqch := pipe.ReceiveChannel()
	req, ok := <-reqch
	if !ok {
		return nil, common.NewError(common.SERVER_ERROR, "SyncProxy.listen(): channel closed. Terminate")		
	}

	if req.Name() != name {
		return nil, common.NewError(common.PROTOCOL_ERROR, 
			"SyncProxy.listen(): Expect message " + name + ", Receive message " + req.Name())
	}

	return req, nil
}

func send(packet common.Packet, pipe *common.PeerPipe) error {

	log.Printf("SyncProxy.send(): sending packet %s to peer (TCP %s)", packet.Name(), pipe.GetAddr())
	if !pipe.Send(packet) {
		return common.NewError(common.SERVER_ERROR, fmt.Sprintf("SyncProxy.listen(): Fail to send packet %s to peer (TCP %s)", 
			packet.Name(), pipe.GetAddr()))		
	}

	return nil
}

func safeSend(header string, donech chan bool, result bool) {
	common.SafeRun(header,
		func() {
			donech <- result 
		})
}
