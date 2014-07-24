package protocol

import (
	"common"
	"sync"
	"fmt"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration 
/////////////////////////////////////////////////////////////////////////////

type LeaderSyncProxy struct {
	state    		*ConsentState
	follower 		*common.PeerPipe
	handler          ActionHandler
	factory          MsgFactory 
	followerState   *FollowerState
}

type FollowerSyncProxy struct {
	leader  		*common.PeerPipe
	handler          ActionHandler
	factory          MsgFactory 
	state           *FollowerState
}

type ConsentState struct {
	acceptedEpoch  		uint32
	acceptedEpochSet	map[string]uint32
	acceptedEpochCond   *sync.Cond	
	acceptedEpochMutex	sync.Mutex
	
	ackEpochSet		    map[string]string
	ackEpochCond   		*sync.Cond	
	ackEpochMutex		sync.Mutex
	
	newLeaderAckSet		map[string]string
	newLeaderAckCond   	*sync.Cond	
	newLeaderAckMutex	sync.Mutex
	
	ensembleSize    	uint64
}

type FollowerState struct {
	currentEpoch		uint32
	lastLoggedTxid		uint64
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

func NewConsentState(sid string, epoch uint32, ensemble uint64) *ConsentState {

	state := &ConsentState{acceptedEpoch : epoch,
	                       acceptedEpochSet : make(map[string]uint32),
	                       ackEpochSet : make(map[string]string),
	                       newLeaderAckSet : make(map[string]string),
	                       ensembleSize : ensemble}
	                       
	state.acceptedEpochCond = sync.NewCond(&state.acceptedEpochMutex)
	state.ackEpochCond = sync.NewCond(&state.ackEpochMutex)
	state.newLeaderAckCond = sync.NewCond(&state.newLeaderAckMutex)

	// add the leader to both sets, since enemble size can count the leader as well.
	state.acceptedEpochSet[sid] = epoch                       
	state.ackEpochSet[sid] = sid 
	state.newLeaderAckSet[sid] = sid 
	           
	return state	
}

func (s *ConsentState) voteAcceptedEpoch(voter string, newEpoch uint32) uint32 {
	s.acceptedEpochCond.L.Lock()
	defer s.acceptedEpochCond.L.Unlock()	

	// Reach quorum. Just Return
	if len(s.acceptedEpochSet) > int(s.ensembleSize / 2) {
		return s.acceptedEpoch
	}
	
	s.acceptedEpochSet[voter] = newEpoch
	
	if newEpoch >= s.acceptedEpoch {
		s.acceptedEpoch = newEpoch + 1
	}

	// TOOD : Use a standard function to check for quorum instead	
	if len(s.acceptedEpochSet) > int(s.ensembleSize / 2) {
		// reach quorum. Notify
		s.acceptedEpochCond.Broadcast()
		return s.acceptedEpoch
	}
	
	// wait for quorum to be reached
	s.acceptedEpochCond.Wait()
	return s.acceptedEpoch	
}

func (s *ConsentState) voteEpochAck(voter string) {
	s.ackEpochCond.L.Lock()
	defer s.ackEpochCond.L.Unlock()	

	// Reach quorum. Just Return
	if len(s.ackEpochSet) > int(s.ensembleSize / 2) {
		return 
	}
	
	s.ackEpochSet[voter] = voter
	
	if len(s.ackEpochSet) > int(s.ensembleSize / 2) {
		// reach quorum. Notify
		s.ackEpochCond.Broadcast()
		return 
	}
	
	// wait for quorum to be reached
	s.ackEpochCond.Wait()
}

func (s *ConsentState) voteNewLeaderAck(voter string) {
	s.newLeaderAckCond.L.Lock()
	defer s.newLeaderAckCond.L.Unlock()	

	// Reach quorum. Just Return
	if len(s.newLeaderAckSet) > int(s.ensembleSize / 2) {
		return 
	}
	
	s.newLeaderAckSet[voter] = voter
	
	if len(s.newLeaderAckSet) > int(s.ensembleSize / 2) {
		// reach quorum. Notify
		s.newLeaderAckCond.Broadcast()
		return 
	}
	
	// wait for quorum to be reached
	s.newLeaderAckCond.Wait()
}

/////////////////////////////////////////////////////////////////////////////
// LeaderSyncProxy 
/////////////////////////////////////////////////////////////////////////////

func NewLeaderSyncProxy(state *ConsentState, 
						follower *common.PeerPipe,
						handler ActionHandler,
						factory MsgFactory) *LeaderSyncProxy {

	sync := &LeaderSyncProxy{state : state,
							 follower : follower,
							 handler : handler,
							 factory : factory}
							 
	return sync
}

func (l* LeaderSyncProxy) Start(donech chan bool) {
	go l.execute(donech)
}

func (l* LeaderSyncProxy) execute(donech chan bool) {

	var stage LeaderStageCode = UPDATE_ACCEPTED_EPOCH_AFTER_QUORUM

	for (stage != LEADER_SYNC_DONE) {
		switch stage {
			case UPDATE_ACCEPTED_EPOCH_AFTER_QUORUM : {
				if l.updateAcceptedEpochAfterQuorum() != nil {
					donech <- false
					return
				}
				stage = NOTIFY_NEW_EPOCH 
			}
			case NOTIFY_NEW_EPOCH : {
				if l.notifyNewEpoch() != nil {
					donech <- false
					return
				}
				stage = UPDATE_CURRENT_EPOCH_AFTER_QUORUM 
			}
			case UPDATE_CURRENT_EPOCH_AFTER_QUORUM : {
				if l.updateCurrentEpochAfterQuorum() != nil {
					donech <- false
					return
				}
				stage = SYNC_SEND
			}	
			case SYNC_SEND : {
				if l.syncWithLeader() != nil {
					donech <- false
					return
				}
				stage = DECLARE_NEW_LEADER_AFTER_QUORUM 
			}
			case DECLARE_NEW_LEADER_AFTER_QUORUM : {
				if l.declareNewLeaderAfterQuorum() != nil {
					donech <- false
					return
				}
				stage = LEADER_SYNC_DONE 
			}
		}
	}
	
	donech <- true
}

func (l *LeaderSyncProxy) updateAcceptedEpochAfterQuorum() error {

	// Get my follower's vote for the accepted epoch 
	packet, err := listen("FollowerInfo", l.follower)	
	if err != nil {
		return err
	}
	
	// Get epoch from follower message		
	info := packet.(FollowerInfoMsg)
	epoch := info.GetAcceptedEpoch()
	
	// update my vote and wait for epoch to reach quorum
	newEpoch := l.state.voteAcceptedEpoch(l.follower.GetAddr(), epoch)
	
	// update the accepted epoch based on the quorum result
	l.handler.NotifyNewAcceptedEpoch(newEpoch)
			
	return nil
}

func (l *LeaderSyncProxy) notifyNewEpoch() error {

	epoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	packet := l.factory.CreateLeaderInfo(epoch)
	return send(packet, l.follower)
}

func (l *LeaderSyncProxy) updateCurrentEpochAfterQuorum() error {

	// Get my follower's vote for the epoch ack 
	packet, err := listen("EpochAck", l.follower)	
	if err != nil {
		return err
	}
	
	// Get epoch from follower message		
	// TODO : Validate follower epoch	
	info := packet.(EpochAckMsg)
	epoch := info.GetCurrentEpoch()
	txid := info.GetLastLoggedTxid()
	l.followerState = &FollowerState{lastLoggedTxid : txid, currentEpoch : epoch}
	
	// update my vote and wait for quorum of ack from followers
	l.state.voteEpochAck(l.follower.GetAddr())
	
	// update the current epock after quorum of followers have ack'ed
	epoch, err = l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	l.handler.NotifyNewCurrentEpoch(epoch)
			
	return nil
}

func (l *LeaderSyncProxy) declareNewLeaderAfterQuorum() error {

	// return the new epoch to the follower
	epoch, err := l.handler.GetAcceptedEpoch()
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
	l.state.voteNewLeaderAck(l.follower.GetAddr())
	
	return nil
}

func (l *LeaderSyncProxy) syncWithLeader() error {

	logChan, errChan, err := l.handler.GetCommitedEntries(l.followerState.lastLoggedTxid)
	if err != nil {
		return err
	} 
	
	for {
		select {
			case entry, ok := <- logChan :
				if !ok {
					// channel close, nothing to send
					return nil
				}
				
 				err = send(entry, l.follower) 
 				if err != nil {
 					// TODO: What to do with the peer?  Need to close the pipe.
 					return  err
 				}
 				
 				// TODO: Need to send the proposal in flight
 				
			case err := <- errChan :
				// TODO : Need to close the pipe
				if err != nil {
					return err
				}
				return nil
		}
	}
	
	return nil
}

/////////////////////////////////////////////////////////////////////////////
// FollowerSyncProxy 
/////////////////////////////////////////////////////////////////////////////

func NewFollowerSyncProxy(leader *common.PeerPipe,
						  handler ActionHandler,
						  factory MsgFactory) *FollowerSyncProxy {

	sync := &FollowerSyncProxy{leader : leader,
							 handler : handler,
							 factory : factory,
							 state : nil}
							 
	return sync
}

func (f* FollowerSyncProxy) Start(donech chan bool) {
	go f.execute(donech)
}

func (l* FollowerSyncProxy) execute(donech chan bool) {

	var stage FollowerStageCode = SEND_FOLLOWERINFO 

	for stage != FOLLOWER_SYNC_DONE	{
		switch stage {
			case SEND_FOLLOWERINFO : {
				if l.sendFollowerInfo() != nil {
					donech <- false
					return
				}
				stage = RECEIVE_UPDATE_ACCEPTED_EPOCH 
			}
			case RECEIVE_UPDATE_ACCEPTED_EPOCH : {
				if l.receiveAndUpdateAcceptedEpoch() != nil {
					donech <- false
					return
				}
				stage = SYNC_RECEIVE 
			}
			case SYNC_RECEIVE : {
				if l.syncReceive() != nil {
					donech <- false
					return
				}
				stage = RECEIVE_UPDATE_CURRENT_EPOCH
			}
			case RECEIVE_UPDATE_CURRENT_EPOCH : {
				if l.receiveAndUpdateCurrentEpoch() != nil {
					donech <- false
					return
				}
				stage = FOLLOWER_SYNC_DONE 
			}
		}
	}
	
	donech <- true
}

func (l *FollowerSyncProxy) sendFollowerInfo() error {

	// Send my accepted epoch to the leader for voting (don't send current epoch)
	epoch, err := l.handler.GetAcceptedEpoch()
	if err != nil {
		return err
	}
	packet := l.factory.CreateFollowerInfo(epoch)
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) receiveAndUpdateAcceptedEpoch() error {

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
		// update the accepted epoch based on the quorum result
		l.handler.NotifyNewAcceptedEpoch(epoch)
	} else {
		return common.NewError(common.PROTOCOL_ERROR, "Accepted Epoch from leader is smaller or equal to my epoch.")
	}	

	// Notify the leader that I have accepted the epoch.  Send
	// the last logged txid and current epoch to the leader.			
	txid := l.handler.GetLastLoggedTxid()
	currentEpoch, err := l.handler.GetCurrentEpoch()
	if err != nil {
		return err
	} 
	l.state = &FollowerState{lastLoggedTxid : uint64(txid), currentEpoch : currentEpoch}
	packet = l.factory.CreateEpochAck(uint64(txid), currentEpoch)
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) receiveAndUpdateCurrentEpoch() error {

	// Get the accepted epoch from the leader.   This epoch
	// is already being voted on by multiple followers (the highest
	// epoch among the quorum of followers).
	packet, err := listen("NewLeader", l.leader)	
	if err != nil {
		return err
	}
	
	// Get epoch from follower message		
	info := packet.(NewLeaderMsg)
	epoch := info.GetEpoch()
	
	// TODO : validate the epoch from leader
	
	// update the accepted epoch based on the quorum result
	l.handler.NotifyNewCurrentEpoch(epoch)

	// Notify the leader that I have accepted the epoch			
	packet = l.factory.CreateNewLeaderAck()
	return send(packet, l.leader)
}

func (l *FollowerSyncProxy) syncReceive() error {

	receiveFirst := false

	for {
		packet, err := listen("LogEntryMsg", l.leader)	
		if err != nil {
			return err
		}
		
		entry := packet.(LogEntryMsg)
		lastLoggedTxnid := entry.GetTxnid()
		
		// If it is the first entry, we expect the entry txid to be the same as my last logged txid
		if !receiveFirst && lastLoggedTxnid != l.state.lastLoggedTxid {
			return common.WrapError(common.PROTOCOL_ERROR, "",  
				fmt.Errorf("Expect to receive first LogEntryMsg with txnid = %d", lastLoggedTxnid))
		}
		
		if !receiveFirst {
			// always skip the first entry since I have this entry in my commit log already
			receiveFirst = true
		} else {
			// if this is not the first entry but the entry's txid is the same as my last logged txid.  It signals the
			// end of the stream.  
			if lastLoggedTxnid == entry.GetTxnid() {
				return nil
			} 
			
			// write the new commit entry
			err = l.handler.CommitEntry(entry.GetTxnid(), entry.GetOpCode(), entry.GetKey(), entry.GetContent())
			if err != nil {
				return err
			}
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
    	// TODO : return error
    }
    
	if req.Name() != name {
		// TODO : return error
	}
	
	return req, nil
}

func send(packet common.Packet, pipe *common.PeerPipe) error {

	if !pipe.Send(packet) {
		// TODO: return error
	}
	
	return nil
}