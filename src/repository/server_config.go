package repository

import (
	"github.com/jliang00/gometa/src/common"
	"strconv"
)

/////////////////////////////////////////////////////////////////////////////
// ServerConfig
/////////////////////////////////////////////////////////////////////////////

type ServerConfig struct {
	repo *Repository
}

/////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

//
// Create a new server config
//
func NewServerConfig(repo *Repository) *ServerConfig {
	config := &ServerConfig{repo: repo}
	config.bootstrap()
	return config
}

func (r *ServerConfig) GetCurrentEpoch() (uint32, error) {
	value, err := r.GetInt(common.CONFIG_CURRENT_EPOCH)
	return uint32(value), err
}

func (r *ServerConfig) GetAcceptedEpoch() (uint32, error) {
	value, err := r.GetInt(common.CONFIG_ACCEPTED_EPOCH)
	return uint32(value), err
}

func (r *ServerConfig) GetLastLoggedTxnId() (uint64, error) {
	value, err := r.GetInt(common.CONFIG_LAST_LOGGED_TXID)
	return value, err
}

func (r *ServerConfig) GetLastCommittedTxnId() (common.Txnid, error) {
	value, err := r.GetInt(common.CONFIG_LAST_COMMITTED_TXID)
	return common.Txnid(value), err
}

func (r *ServerConfig) SetCurrentEpoch(epoch uint32) error {
	err := r.LogInt(common.CONFIG_CURRENT_EPOCH, uint64(epoch))
	if err != nil {
		return err
	}
	return nil
}

func (r *ServerConfig) SetAcceptedEpoch(epoch uint32) error {
	err := r.LogInt(common.CONFIG_ACCEPTED_EPOCH, uint64(epoch))
	if err != nil {
		return err
	}
	return nil
}

func (r *ServerConfig) SetLastLoggedTxid(lastLoggedTxid common.Txnid) error {
	err := r.LogInt(common.CONFIG_LAST_LOGGED_TXID, uint64(lastLoggedTxid))
	if err != nil {
		return err
	}
	return nil
}

func (r *ServerConfig) SetLastCommittedTxid(lastCommittedTxid common.Txnid) error {
	err := r.LogInt(common.CONFIG_LAST_COMMITTED_TXID, uint64(lastCommittedTxid))
	if err != nil {
		return err
	}
	return nil
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
		return "", common.WrapError(common.SERVER_CONFIG_ERROR, "Key = "+key, err)
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
		return 0, common.WrapError(common.SERVER_CONFIG_ERROR, "Key = "+key, err)
	}

	return strconv.ParseUint(string(data), 10, 64)
}

//
// Delete from server config 
//
func (r *ServerConfig) Delete(key string) error {

	k := createConfigKey(key)
	return r.repo.Delete(k)
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func createConfigKey(key string) string {
	return common.PREFIX_SERVER_CONFIG_PATH + key
}

func (r *ServerConfig) bootstrap() {
	value, err := r.GetInt(common.CONFIG_MAGIC)
	if err != nil || value != common.CONFIG_MAGIC_VALUE {
		r.LogInt(common.CONFIG_MAGIC, common.CONFIG_MAGIC_VALUE)
		r.SetCurrentEpoch(common.BOOTSTRAP_CURRENT_EPOCH)
		r.SetAcceptedEpoch(common.BOOTSTRAP_ACCEPTED_EPOCH)
		r.SetLastLoggedTxid(common.BOOTSTRAP_LAST_LOGGED_TXID)
		r.SetLastCommittedTxid(common.BOOTSTRAP_LAST_COMMITTED_TXID)
	}
}
