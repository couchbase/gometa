package common 

import (
	"sync"
)

type Txnid uint64

var gEpoch   uint64 = 0
var gCounter uint64 = 0
var gMutex   sync.Mutex 

var mask     uint64 = 0x00000000FFFFFFFF

func GetNextTxnId() Txnid {
	gMutex.Lock()
	defer gMutex.Unlock()
	
	// Increment the epoch if the counter overflows.
	// This is equivalent to a new leader being selected.
	// Note that we check 0xFFFFFFFE so that if we do 
	// (Txnid + 1), it will not overflow. 
	// TODO: Need to check if we need to resync the state if this happens.
	// TODO: Need to see if we need to make the epoch and id volatile 
	gCounter++
	if gCounter == 0xFFFFFFFE { 
		gEpoch++
		gCounter = 0
	}

	epoch := gEpoch << 32
	return Txnid(epoch + gCounter)
}

func (id Txnid) GetEpoch() uint64 {
	v := uint64(id)
	return  (v >> 32)
}

func (id Txnid) GetCounter() uint64 {
	v := uint64(id)
	return (v | mask)
}