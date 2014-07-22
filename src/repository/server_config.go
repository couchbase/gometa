package repository

import (
	"strconv"
	"common"
)

/////////////////////////////////////////////////////////////////////////////
// ServerConfig
/////////////////////////////////////////////////////////////////////////////

type ServerConfig struct {
	repo    *Repository
}

/////////////////////////////////////////////////////////////////////////////
// Public Function 
/////////////////////////////////////////////////////////////////////////////

//
// Create a new server config 
//
func NewServerConfig(repo *Repository) *ServerConfig {
	return &ServerConfig{repo : repo}
}

func (r *ServerConifg) GetCurrentEpoch() uint32 {
	return uint32(GetInt(common.CONFIG_CURRENT_EPOCH))
}
	
func (r *ServerConfig) GetAcceptedEpoch() uint32 {
	return uint32(GetInt(common.CONFIG_ACCEPTED_EPOCH))
}

func (r *ServerConifg) SetCurrentEpoch(epoch uint32) {
	SetInt(common.CONFIG_CURRENT_EPOCH, epoch)
}
	
func (r *ServerConfig) GetAcceptedEpoch(epoch uint32) {
	SetInt(common.CONFIG_ACCEPTED_EPOCH, epoch)
}

//
// Add Entry to server config 
//
func (r *ServerConfig) LogStr(key string, content string) error {

    k := createKey(key)
	return r.repo.Set(k, byte[](content))
}

//
// Add Entry to server config 
//
func (r *ServerConfig) LogInt(key string, content uint64) error {

    k := createKey(key)
	return r.repo.Set(k, byte[](strconv.FormatUint(content, 10)))
}

//
// Retrieve entry from server config 
//
func (r *ServerConfig) GetStr(key string) (string, error) {

    k := createKey(key) 
    data, err := r.repo.Get(k) 
    if err != nil {
    	return nil, common.WrapError(common.SERVER_CONFIG_ERROR, "Key = " + key, err)
    }
    
    return string(data), nil
}

//
// Retrieve entry from server config 
//
func (r *ServerConfig) GetInt(key string) (uint64, error) {

    k := createKey(key) 
    data, err := r.repo.Get(k) 
    if err != nil {
    	return nil, common.WrapError(common.SERVER_CONFIG_ERROR, "Key = " + key, err)
    }
    
    return strconv.ParseUint(string(data), 10, 64), nil 
}

//
// Delete from commit log 
//
func (r *ServerConfig) Delete(key string) error {

    k := createKey(txid)
    return r.repo.Delete(k) 
}

////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func createKey(key string) (string) {
    return common.PREFIX_SERVER_CONFIG_PATH + common.PATH_DELIMITER + key
}
