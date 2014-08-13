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
	if gCounter == 0xFFFFFFFF {
		panic(fmt.Sprintf("Counter overflows for epoch %d", gEpoch))
	}
	gCounter++

	epoch := uint64(gEpoch << 32)
	return Txnid(epoch + gCounter)
}

func SetEpoch(newEpoch uint32) {
	gMutex.Lock()
	defer gMutex.Unlock()
	
	// TODO : if epoch is smaller than the old value 
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
