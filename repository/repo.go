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

package repository

import (
	"github.com/couchbase/gometa/common"
	fdb "github.com/couchbaselabs/goforestdb"
	"log"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// Repository
/////////////////////////////////////////////////////////////////////////////

type Repository struct {
	dbfile    *fdb.File
	db        *fdb.KVStore
	snapshots []*Snapshot
	mutex     sync.Mutex
}

type RepoIterator struct {
	iter *fdb.Iterator
	db *fdb.KVStore
}

type Snapshot struct {
	snapshot *fdb.KVStore
	count    int
	txnid    common.Txnid
	mutex    sync.Mutex
}

/////////////////////////////////////////////////////////////////////////////
// Repository Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Open a repository
//
func OpenRepository() (*Repository, error) {
	return OpenRepositoryWithName(common.REPOSITORY_NAME)
}

func OpenRepositoryWithName(name string) (*Repository, error) {

	config := fdb.DefaultConfig()
	config.SetBufferCacheSize(1024 * 1024)
	dbfile, err := fdb.Open(name, config)
	if err != nil {
		return nil, err
	}

	cleanup := common.NewCleanup(func() {
		dbfile.Close()
	})
	defer cleanup.Run()

	db, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		return nil, err
	}
	cleanup.Cancel()

	repo := &Repository{dbfile: dbfile, db: db, snapshots: nil}
	return repo, nil
}

//
// Update/Insert into the repository
//
func (r *Repository) Set(key string, content []byte) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Printf("Repo.Set(): key %s, len(content) %d", key, len(content))

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	// set value
	err = r.db.SetKV(k, content)
	if err != nil {
		return err
	}

	return r.dbfile.Commit(fdb.COMMIT_NORMAL)
}

func (r *Repository) CreateSnapshot(txnid common.Txnid) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	info, err := r.db.Info()
	if err != nil {
		return err
	}

	fdbSnapshot, err := r.db.SnapshotOpen(info.LastSeqNum())
	if err != nil {
		return err
	}

	snapshot := &Snapshot{snapshot: fdbSnapshot,
		txnid: txnid,
		count: 0}

	r.pruneSnapshot()

	r.snapshots = append(r.snapshots, snapshot)
	return nil
}

func (r *Repository) AcquireSnapshot() (common.Txnid, *RepoIterator, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.snapshots) == 0 {
		return common.Txnid(0), nil, nil
	}

	snapshot := r.snapshots[len(r.snapshots)-1]

	iter, err := snapshot.snapshot.IteratorInit(nil, nil, fdb.ITR_NONE)
	if err != nil {
		return common.Txnid(0), nil, err
	}
	return snapshot.txnid, &RepoIterator{iter: iter}, nil
}

func (r *Repository) ReleaseSnapshot(txnid common.Txnid) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, snapshot := range r.snapshots {
		if snapshot.txnid == txnid && snapshot.count > 0 {
			snapshot.count--
		}
	}
}

func (r *Repository) pruneSnapshot() {

	// TODO : closing a snapshot
	var newList []*Snapshot = nil
	for _, snapshot := range r.snapshots {
		if snapshot.count <= 0 {
			newList = append(newList, snapshot)
		} else {
			// closing snapshot
			snapshot.snapshot.Close()
		}
	}

	r.snapshots = newList
}

//
// Update/Insert into the repository
//
func (r *Repository) SetNoCommit(key string, content []byte) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Printf("Repo.SetNoCommit(): key %s, len(content) %d", key, len(content))

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	// set value
	return r.db.SetKV(k, content)
}

//
// Retrieve from repository
//
func (r *Repository) Get(key string) ([]byte, error) {

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return nil, err
	}

	value, err := r.db.GetKV(k)
	log.Printf("Repo.Get(): key %s, found=%v", key, err == nil)
	return value, err
}

//
// Delete from repository
//
func (r *Repository) Delete(key string) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	err = r.db.DeleteKV(k)
	if err != nil {
		return err
	}

	return r.dbfile.Commit(fdb.COMMIT_NORMAL)
}

//
// Delete from repository
//
func (r *Repository) DeleteNoCommit(key string) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	return r.db.DeleteKV(k)
}

//
// Delete from repository
//
func (r *Repository) Commit() error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.dbfile.Commit(fdb.COMMIT_NORMAL)
}

//
// Close repository.
//
func (r *Repository) Close() {
	// TODO: Does it need mutex?
	if r.db != nil {
		for _, snapshot := range r.snapshots {
			snapshot.snapshot.Close()
		}
		r.snapshots = nil

		r.db.Close()
		r.db = nil

		r.dbfile.Close()
		r.dbfile = nil
	}
}

/////////////////////////////////////////////////////////////////////////////
// RepoIterator Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator.  EndKey is inclusive.
//
func (r *Repository) NewIterator(startKey, endKey string) (*RepoIterator, error) {
	// TODO: Check if fdb is closed.

	k1, err := CollateString(startKey)
	if err != nil {
		return nil, err
	}

	k2, err := CollateString(endKey)
	if err != nil {
		return nil, err
	}

	iter, err := r.db.IteratorInit(k1, k2, fdb.ITR_NONE)
	if err != nil {
		return nil, err
	}
	result := &RepoIterator{iter: iter, db : r.db}
	return result, nil
}

// Get value from iterator
func (i *RepoIterator) Next() (key string, content []byte, err error) {

	if i.iter == nil {
		return "", nil, fdb.RESULT_ITERATOR_FAIL 
	}

	doc, err := i.iter.Get()
	if err != nil {
		return "", nil, err
	}

	err = i.iter.Next()
	if err != nil && err != fdb.RESULT_ITERATOR_FAIL {
		return "", nil, err
	}

	i.db.Get(doc)
	key = DecodeString(doc.Key())
	body := doc.Body()

	if err == fdb.RESULT_ITERATOR_FAIL {
		i.iter = nil
	}

	return key, body, nil
}

// close iterator
func (i *RepoIterator) Close() {
	// TODO: Check if fdb iterator is closed
	i.iter.Close()
	i.iter = nil
}

// This only support ascii.
func CollateString(key string) ([]byte, error) {
	if key == "" {
		return nil, nil
	}

	return ([]byte)(key), nil
}

func DecodeString(data []byte) string {
	return string(data)
}
