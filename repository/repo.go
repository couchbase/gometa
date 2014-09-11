package repository

import (
	"github.com/couchbase/gometa/common"
	"log"
	fdb "github.com/couchbaselabs/goforestdb"
	"sync"
)

/////////////////////////////////////////////////////////////////////////////
// Repository
/////////////////////////////////////////////////////////////////////////////

type Repository struct {
	db    *fdb.Database
	mutex sync.Mutex
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

	db, err := fdb.Open(common.REPOSITORY_NAME, nil)
	if err != nil {
		return nil, err
	}

	repo := &Repository{db: db}
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

	return r.db.Commit(fdb.COMMIT_NORMAL)
}

//
// Retrieve from repository
//
func (r *Repository) Get(key string) ([]byte, error) {

	log.Printf("Repo.Get(): key %s", key)
	
	//convert key to its collatejson encoded byte representation
	k, err := CollateString(key)
	if err != nil {
		return nil, err
	}

	return r.db.GetKV(k)
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
	
	return r.db.Commit(fdb.COMMIT_NORMAL)
}

//
// Close repository.
//
func (r *Repository) Close() {
	// TODO: Does it need mutex?
	if r.db != nil {
		r.db.Close()
		r.db = nil
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
