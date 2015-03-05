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
	"github.com/couchbase/gometa/log"
	fdb "github.com/couchbaselabs/goforestdb"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// Repository
/////////////////////////////////////////////////////////////////////////////

type RepoKind int

const (
	MAIN RepoKind = iota
	COMMIT_LOG
	SERVER_CONFIG
	LOCAL
)

type Repository struct {
	dbfile    *fdb.File
	stores    map[RepoKind]*fdb.KVStore
	snapshots map[RepoKind][]*Snapshot
	mutex     sync.Mutex
}

type RepoIterator struct {
	iter  *fdb.Iterator
	store *fdb.KVStore
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
	return OpenRepositoryWithName(common.REPOSITORY_NAME, uint64(0))
}

func OpenRepositoryWithName(name string, memory_quota uint64) (repo *Repository, err error) {

	if memory_quota < common.MIN_FOREST_DB_CACHE_SIZE {
		memory_quota = common.MIN_FOREST_DB_CACHE_SIZE
	}

	log.Current.Debugf("Repo.OpenRepositoryWithName(): open repo with name %s, buffer cache size %d", name, memory_quota)

	config := fdb.DefaultConfig()
	config.SetBufferCacheSize(memory_quota)
	dbfile, err := fdb.Open(name, config)
	if err != nil {
		return nil, err
	}

	cleanup := common.NewCleanup(func() {
		dbfile.Close()
	})
	defer cleanup.Run()

	stores := make(map[RepoKind]*fdb.KVStore)

	if stores[MAIN], err = dbfile.OpenKVStore("MAIN", nil); err != nil {
		return nil, err
	}
	if stores[COMMIT_LOG], err = dbfile.OpenKVStore("COMMIT_LOG", nil); err != nil {
		return nil, err
	}
	if stores[SERVER_CONFIG], err = dbfile.OpenKVStore("SERVER_CONFIG", nil); err != nil {
		return nil, err
	}
	if stores[LOCAL], err = dbfile.OpenKVStore("LOCAL", nil); err != nil {
		return nil, err
	}
	cleanup.Cancel()

	snapshots := make(map[RepoKind][]*Snapshot)
	snapshots[MAIN] = nil
	snapshots[COMMIT_LOG] = nil
	snapshots[SERVER_CONFIG] = nil
	snapshots[LOCAL] = nil

	repo = &Repository{dbfile: dbfile,
		stores:    stores,
		snapshots: snapshots}

	return repo, nil
}

//
// Update/Insert into the repository
//
func (r *Repository) Set(kind RepoKind, key string, content []byte) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Current.Debugf("Repo.Set(): key %s, len(content) %d", key, len(content))

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	// set value
	err = r.stores[kind].SetKV(k, content)
	if err != nil {
		return err
	}

	return r.dbfile.Commit(fdb.COMMIT_NORMAL)
}

func (r *Repository) CreateSnapshot(kind RepoKind, txnid common.Txnid) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	info, err := r.stores[kind].Info()
	if err != nil {
		return err
	}

	fdbSnapshot, err := r.stores[kind].SnapshotOpen(info.LastSeqNum())
	if err != nil {
		return err
	}

	snapshot := &Snapshot{snapshot: fdbSnapshot,
		txnid: txnid,
		count: 0}

	r.pruneSnapshot(kind)

	r.snapshots[kind] = append(r.snapshots[kind], snapshot)

	log.Current.Debugf("Repo.CreateSnapshot(): txnid %v, forestdb seqnum %v", txnid, info.LastSeqNum())
	return nil
}

func (r *Repository) AcquireSnapshot(kind RepoKind) (common.Txnid, *RepoIterator, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.snapshots[kind]) == 0 {
		return common.Txnid(0), nil, nil
	}

	snapshot := r.snapshots[kind][len(r.snapshots[kind])-1]
	snapshot.count++

	iter, err := snapshot.snapshot.IteratorInit(nil, nil, fdb.ITR_NO_DELETES)
	if err != nil {
		return common.Txnid(0), nil, err
	}
	return snapshot.txnid, &RepoIterator{iter: iter}, nil
}

func (r *Repository) ReleaseSnapshot(kind RepoKind, txnid common.Txnid) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, snapshot := range r.snapshots[kind] {
		if snapshot.txnid == txnid && snapshot.count > 0 {
			snapshot.count--
		}
	}
}

func (r *Repository) pruneSnapshot(kind RepoKind) {

	var newList []*Snapshot = nil
	for _, snapshot := range r.snapshots[kind] {
		if snapshot.count > 0 {
			newList = append(newList, snapshot)
		} else {
			// closing snapshot
			snapshot.snapshot.Close()
		}
	}

	r.snapshots[kind] = newList
}

//
// Update/Insert into the repository
//
func (r *Repository) SetNoCommit(kind RepoKind, key string, content []byte) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Current.Debugf("Repo.SetNoCommit(): key %s, len(content) %d", key, len(content))

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	// set value
	return r.stores[kind].SetKV(k, content)
}

//
// Retrieve from repository
//
func (r *Repository) Get(kind RepoKind, key string) ([]byte, error) {

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return nil, err
	}

	value, err := r.stores[kind].GetKV(k)
	log.Current.Debugf("Repo.Get(): key %s, found=%v", key, err == nil)
	return value, err
}

//
// Delete from repository
//
func (r *Repository) Delete(kind RepoKind, key string) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	err = r.stores[kind].DeleteKV(k)
	if err != nil {
		return err
	}

	return r.dbfile.Commit(fdb.COMMIT_NORMAL)
}

//
// Delete from repository
//
func (r *Repository) DeleteNoCommit(kind RepoKind, key string) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return err
	}

	return r.stores[kind].DeleteKV(k)
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
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.dbfile != nil {
		for _, snapshot := range r.snapshots[MAIN] {
			snapshot.snapshot.Close()
		}
		for _, snapshot := range r.snapshots[COMMIT_LOG] {
			snapshot.snapshot.Close()
		}
		for _, snapshot := range r.snapshots[SERVER_CONFIG] {
			snapshot.snapshot.Close()
		}
		for _, snapshot := range r.snapshots[LOCAL] {
			snapshot.snapshot.Close()
		}
		r.snapshots = nil

		for _, store := range r.stores {
			store.Close()
		}
		r.stores = nil

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
func (r *Repository) NewIterator(kind RepoKind, startKey, endKey string) (*RepoIterator, error) {
	// TODO: Check if fdb is closed.

	k1, err := CollateString(startKey)
	if err != nil {
		return nil, err
	}

	k2, err := CollateString(endKey)
	if err != nil {
		return nil, err
	}

	iter, err := r.stores[kind].IteratorInit(k1, k2, fdb.ITR_NO_DELETES)
	if err != nil {
		return nil, err
	}
	result := &RepoIterator{iter: iter, store: r.stores[kind]}
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

	//i.db.Get(doc)
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
	if i.iter != nil {
		i.iter.Close()
		i.iter = nil
	}
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
