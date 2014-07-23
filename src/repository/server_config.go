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

func (r *ServerConfig) GetCurrentEpoch() (uint32, error) {
	value, err :=  r.GetInt(common.CONFIG_CURRENT_EPOCH)
	return uint32(value), err
}
	
func (r *ServerConfig) GetAcceptedEpoch() (uint32, error) {
	value, err := r.GetInt(common.CONFIG_ACCEPTED_EPOCH)
	return uint32(value), err
}

func (r *ServerConfig) SetCurrentEpoch(epoch uint32) {
	r.LogInt(common.CONFIG_CURRENT_EPOCH, uint64(epoch))
}
	
func (r *ServerConfig) SetAcceptedEpoch(epoch uint32) {
	r.LogInt(common.CONFIG_ACCEPTED_EPOCH, uint64(epoch))
}

//
// Add Entry to server config 
//
func (r *ServerConfig) LogStr(key string, content string) error {

    k := createConfigKey(key)
	return r.repo.Set(k, []byte(content))
}

//
// Add Entry to server config 
//
func (r *ServerConfig) LogInt(key string, content uint64) error {

    k := createConfigKey(key)
	return r.repo.Set(k, []byte(strconv.FormatUint(content, 10)))
}

//
// Retrieve entry from server config 
//
func (r *ServerConfig) GetStr(key string) (string, error) {

    k := createConfigKey(key) 
    data, err := r.repo.Get(k) 
    if err != nil {
    	return "", common.WrapError(common.SERVER_CONFIG_ERROR, "Key = " + key, err)
    }
    
    return string(data), nil 
}

//
// Retrieve entry from server config 
//
func (r *ServerConfig) GetInt(key string) (uint64, error) {

    k := createConfigKey(key) 
    data, err := r.repo.Get(k) 
    if err != nil {
    	return 0, common.WrapError(common.SERVER_CONFIG_ERROR, "Key = " + key, err)
    }
    
    return strconv.ParseUint(string(data), 10, 64)
}

//
// Delete from commit log 
//
func (r *ServerConfig) Delete(key string) error {

    k := createConfigKey(key)
    return r.repo.Delete(k) 
}

////////////////////////////////////////////////////////////////////////////
// Private Function 
/////////////////////////////////////////////////////////////////////////////

func createConfigKey(key string) (string) {
    return common.PREFIX_SERVER_CONFIG_PATH + common.PATH_DELIMITER + key
}
