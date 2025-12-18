// Package repository contains implementation to store local metadata. this file contains
// the interfaces for it
package repository

import (
	"fmt"

	c "github.com/couchbase/gometa/common"
)

type IRepoIterator interface {
	Close()
	Next() (key string, content []byte, err error)
}

type IRepository interface {
	Set(kind RepoKind, key string, content []byte) error
	CreateSnapshot(kind RepoKind, txnid c.Txnid) error
	AcquireSnapshot(kind RepoKind) (c.Txnid, IRepoIterator, error)
	ReleaseSnapshot(kind RepoKind, txnid c.Txnid)
	SetNoCommit(kind RepoKind, key string, content []byte) error
	Get(kind RepoKind, key string) ([]byte, error)
	Delete(kind RepoKind, key string) error
	DeleteNoCommit(kind RepoKind, key string) error
	Commit() error
	Close()
	NewIterator(kind RepoKind, startKey, endKey string) (IRepoIterator, error)
	Type() StoreType
}

type StoreType uint8

const (
	NAStoreType StoreType = iota
	FDbStoreType
	MagmaStoreType
)

func (sType StoreType) String() string {
	switch sType {
	case FDbStoreType:
		return "fdb"
	case MagmaStoreType:
		return "magma"
	default:
		return "unknown"
	}
}

type StoreErrorCode int

func (sec StoreErrorCode) Error() string {
	codeStr, ok := errCodeMap[sec]
	if ok {
		return codeStr
	}
	return ""
}

type StoreError struct {
	errMsg    string
	sType     StoreType
	storeCode StoreErrorCode
	rawErr    interface{} // rawErr from the underlying store
}

func (sErr StoreError) Code() StoreErrorCode {
	return sErr.storeCode
}

func (sErr StoreError) String() string {
	msg := fmt.Sprintf("[%s]%v", sErr.sType, sErr.errMsg)
	if sErr.storeCode != 0 {
		codeStr := sErr.storeCode.Error()
		if len(codeStr) > 0 {
			msg = fmt.Sprintf("<%s>:%s", sErr.storeCode.Error(), msg)
		} else {
			msg = fmt.Sprintf("<%d>:%s", sErr.storeCode, msg)
		}
	}
	return msg
}

func (sErr StoreError) Error() string {
	return sErr.String()
}

func (sErr StoreError) Is(err error) bool {
	cErr, ok := err.(*StoreError)
	if ok && cErr != nil {
		return sErr.Code() == cErr.Code()
	}
	c2Err, ok := err.(StoreError)
	if ok {
		return c2Err.Code() == sErr.Code()
	}
	return false
}

const (
	ErrRepoClosedCode StoreErrorCode = iota + 1
	ErrIterFailCode
	ErrResultNotFoundCode
	ErrNotSupported
	ErrInternalError StoreErrorCode = -1
)

var errCodeMap = map[StoreErrorCode]string{
	ErrRepoClosedCode:     "ERR_REPO_CLOSED",
	ErrIterFailCode:       "ERR_ITERATOR_FAIL",
	ErrResultNotFoundCode: "ERR_RESULT_NOT_FOUND",
	ErrNotSupported:       "ERR_NOT_SUPPORTED",
}

type RepoFactoryParams struct {
	Dir                        string
	MemoryQuota                uint64
	CompactionTimerDur         uint64
	CompactionMinFileSize      uint64
	CompactionThresholdPercent uint8
	StoreType                  StoreType
	EnableWAL                  bool
}

func (params RepoFactoryParams) String() string {
	return fmt.Sprintf("{dir: %s; quota: %d MB; compact_timer: %d s; min_file_size: %d b; use_wal: %v; store: %v}",
		params.Dir, params.MemoryQuota/(1024*1024), params.CompactionTimerDur,
		params.CompactionMinFileSize, params.EnableWAL, params.StoreType,
	)
}

func OpenOrCreateNewRepositoryFromParams(params RepoFactoryParams) (IRepository, error) {
	switch params.StoreType {
	case MagmaStoreType:
		return OpenMagmaRepositoryAndUpgrade(params)
	case FDbStoreType:
		return OpenFDbRepositoryWithParams(params)
	}
	return nil, &StoreError{
		sType:     params.StoreType,
		storeCode: ErrNotSupported,
	}
}
