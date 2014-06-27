package repository

import (
	// TODO
	//fdb "github.com/couchbaselabs/goforestdb"
	"github.com/prataprc/collatejson"
	"bytes"
)

/////////////////////////////////////////////////////////////////////////////
// Repository 
/////////////////////////////////////////////////////////////////////////////

type Repository struct {
	// TODO
	/*db 		*fdb.Database */
	db          FakeDB
}

type FakeDB interface {
	SetKV(key []byte, content []byte)	error
	GetKV(key []byte)	([]byte, error)
	DeleteKV(key []byte)	error
	Close()	error
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Open a repository
//
func OpenRepository() (*Repository, error) {
	
	//db, err := fdb.Open(common.REPOSITORY_NAME, nil)	
	var db FakeDB = nil
	//if err != nil {
	//	return nil, err
	//}
	
	repo := &Repository{db : db}
	return repo, nil
}

//
// Update/Insert into the repository 
//
func (r *Repository) Set(key string, content []byte) error {

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
    k, err := buf.Write(jsoncodec.EncodeString(key))
    if err != nil {
    	return nil, err
    }
   	return buf.Bytes(), nil 
}

