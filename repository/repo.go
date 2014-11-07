// @author Couchbase <info@couchbase.com>
// @copyright 2014 NorthScale, Inc.
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
	dbfile *fdb.File
	db     *fdb.KVStore
	mutex  sync.Mutex
}

type RepoIterator struct {
	iter *fdb.Iterator
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

	repo := &Repository{dbfile: dbfile, db: db}
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
// Close repository.
//
func (r *Repository) Close() {
	// TODO: Does it need mutex?
	if r.db != nil {
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
	result := &RepoIterator{iter: iter}
	return result, nil
}

// Get value from iterator
func (i *RepoIterator) Next() (key string, content []byte, err error) {

	// TODO: Check if fdb and iterator is closed
	doc, err := i.iter.Next()
	if err != nil {
		return "", nil, err
	}

	key = DecodeString(doc.Key())
	body := doc.Body()

	return key, body, nil
}

// close iterator
func (i *RepoIterator) Close() {
	// TODO: Check if fdb iterator is closed
	i.iter.Close()
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
