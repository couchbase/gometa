package repository

import (
	// TODO
	//fdb "github.com/couchbaselabs/goforestdb"
	"github.com/prataprc/collatejson"
)

/////////////////////////////////////////////////////////////////////////////
// Repository 
/////////////////////////////////////////////////////////////////////////////

type Repository struct {
	// TODO
	/*db 		*fdb.Database */
	db          *FakeDB
}

type FakeDB interface {
	func SetKV(key, content []byte)	error
	func GetKV(key []byte)	([]byte, error)
	func DeleteKV(key []byte)	error
	func Close()	error
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Open a repository
//
func OpenRepository() (*Repository, error) {
	
	//db, err := fdb.Open(common.REPOSITORY_NAME, nil)	
	db := nil
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

    //convert key to its collatejson encoded byte representation
    k := collateString(key)

	// set value 
	return r.db.SetKV(k, content)	
}

//
// Retrieve from repository 
//
func (r *Repository) Get(key string) (byte[], error) {

    //convert key to its collatejson encoded byte representation
    k := collateString(key)
    
    return r.db.GetKV(k) 
}

//
// Delete from repository 
//
func (r *Repository) Delete(key string) error {

    //convert key to its collatejson encoded byte representation
    k := collateString(key)
    
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
    k, err = buf.Write(jsoncodec.EncodeString(key))
    if err != nil {
    	return nil, err
    }
   	return k, nil 
}

