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

package common

import (
	"fmt"
	"sync"
)

type Txnid uint64

type TxnState struct {
	epoch    uint64
	counter  uint64
	mutex    sync.Mutex
	curTxnid Txnid
}

func NewTxnState() *TxnState {
	state := new(TxnState)
	state.epoch = 0
	state.counter = 0
	state.curTxnid = 0

	return state
}

func (t *TxnState) GetNextTxnId() Txnid {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Increment the epoch. If the counter overflows, panic.
	if t.counter == uint64(MAX_COUNTER) {
		panic(fmt.Sprintf("Counter overflows for epoch %d", t.epoch))
	}
	t.counter++

	epoch := uint64(t.epoch << 32)
	newTxnid := Txnid(epoch + t.counter)

	// t.curTxnid is initialized using the LastLoggedTxid in the local repository.  So if this node becomes master,
	// we want to make sure that the new txid is larger than the one that we saw before.
	if t.curTxnid >= newTxnid {
		// Assertion.  This is to ensure integrity of the system.  Wrong txnid can result in corruption.
		panic(fmt.Sprintf("GetNextTxnId(): Assertion: New Txnid %d is smaller than or equal to old txnid %d", newTxnid, t.curTxnid))
	}

	t.curTxnid = newTxnid

	return t.curTxnid
}

// Return true if txid2 is logically next in sequence from txid1.
// If txid2 and txid1 have different epoch, then only check if
// txid2 has a larger epoch.  Otherwise, compare the counter such
// that txid2 is txid1 + 1
func IsNextInSequence(new, old Txnid) bool {

	if new.GetEpoch() > old.GetEpoch() {
		return true
	}

	if new.GetEpoch() == old.GetEpoch() &&
		uint32(old.GetCounter()) != MAX_COUNTER &&
		new == old+1 {
		return true
	}

	return false
}

func (t *TxnState) SetEpoch(newEpoch uint32) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.epoch >= uint64(newEpoch) {
		// Assertion.  This is to ensure integrity of the system.  We do not support epoch rollover yet.
		panic(fmt.Sprintf("SetEpoch(): Assertion: New Epoch %d is smaller than or equal to old epoch %d", newEpoch, t.epoch))
	}

	t.epoch = uint64(newEpoch)
	t.counter = 0
}

func (t *TxnState) InitCurrentTxnid(txnid Txnid) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if txnid > t.curTxnid {
		t.curTxnid = txnid
	}
}

func (id Txnid) GetEpoch() uint64 {
	v := uint64(id)
	return (v >> 32)
}

func (id Txnid) GetCounter() uint64 {
	v := uint32(id)
	return uint64(v)
}

//
// Compare function to compare epoch1 with epoch2
//
// return common.EQUAL if epoch1 is the same as epoch2
// return common.MORE_RECENT if epoch1 is more recent
// return common.LESS_RECENT if epoch1 is less recent
//
// This is just to prepare in the future if we support
// rolling over the epoch (but it will also require
// changes to comparing txnid as well).
//
func CompareEpoch(epoch1, epoch2 uint32) CompareResult {

	if epoch1 == epoch2 {
		return EQUAL
	}

	if epoch1 > epoch2 {
		return MORE_RECENT
	}

	return LESS_RECENT
}

//
// Compare epoch1 and epoch2.  If epoch1 is equal or more recent, return
// the next more recent epoch value.   If epoch1 is less recent than
// epoch2, return epoch2 as it is.
//
func CompareAndIncrementEpoch(epoch1, epoch2 uint32) uint32 {

	result := CompareEpoch(epoch1, epoch2)
	if result == MORE_RECENT || result == EQUAL {
		if epoch1 != MAX_EPOCH {
			return epoch1 + 1
		}

		// TODO : Epoch has reached the max value. If we have a leader
		// election every second, it will take 135 years to overflow the epoch (32 bits).
		// Regardless, we should gracefully roll over the epoch eventually in our
		// implementation.
		panic("epoch limit is reached")
	}

	return epoch2
}
