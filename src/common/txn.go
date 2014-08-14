package common

import (
	"sync"
	"fmt"
)

type Txnid uint64

var gEpoch uint64 = 0
var gCounter uint64 = 0
var gMutex sync.Mutex

func GetNextTxnId() Txnid {
	gMutex.Lock()
	defer gMutex.Unlock()

	// Increment the epoch. If the counter overflows, panic.
	if gCounter == uint64(MAX_COUNTER) {
		panic(fmt.Sprintf("Counter overflows for epoch %d", gEpoch))
	}
	gCounter++

	//TODO: double check
	epoch := uint64(gEpoch << 32)
	return Txnid(epoch + gCounter)
}

func SetEpoch(newEpoch uint32) {
	gMutex.Lock()
	defer gMutex.Unlock()

	// Do not need to check if the newEpoch is larger.
	gEpoch = uint64(newEpoch)
	gCounter = 0
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
