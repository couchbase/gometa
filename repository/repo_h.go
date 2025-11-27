// Package repository contains implementation to store local metadata. this file contains
// the interfaces for it
package repository

import (
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
}
