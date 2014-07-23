package repository

import (
	fdb "github.com/couchbaselabs/goforestdb"
	"github.com/prataprc/collatejson"
	"bytes"
	"sync"
	"common"
)

/////////////////////////////////////////////////////////////////////////////
// Repository 
/////////////////////////////////////////////////////////////////////////////

type Repository struct {
	db 		*fdb.Database 
	mutex   sync.Mutex
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Open a repository
//
func OpenRepository() (*Repository, error) {
	
	db, err := fdb.Open(common.REPOSITORY_NAME, nil)	
	if err != nil {
		return nil, err
	}
	
	repo := &Repository{db : db}
	return repo, nil
}

//
// Update/Insert into the repository 
//
func (r *Repository) Set(key string, content []byte) error {

	r.mutex.Lock()
	defer r.mutex.Unlock()

    //convert key to its collatejson encoded byte representation
    k, err := collateString(key)
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
    k, err := collateString(key)
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
    k, err := collateString(key)
    if err != nil {
    	return err
    }
    
    return r.db.DeleteKV(k) 
}

//
// Close repository.  
//
func (r *Repository) Close() {
	if r.db != nil {
		r.db.Close()	
		r.db = nil	
	}
}

/////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func collateString(key string) ([]byte, error) {
    jsoncodec := collatejson.NewCodec()
    buf := new(bytes.Buffer)
    _, err := buf.Write(jsoncodec.EncodeString(key))
    if err != nil {
    	return nil, err
    }
   	return buf.Bytes(), nil 
}

