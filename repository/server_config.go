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
	"fmt"
	"strconv"

	"github.com/couchbase/gometa/common"
)

/////////////////////////////////////////////////////////////////////////////
// ServerConfig
/////////////////////////////////////////////////////////////////////////////

type ServerConfig struct {
	repo IRepository
}

/////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

// Create a new server config
func NewServerConfig(repo IRepository) *ServerConfig {
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

// Add Entry to server config
func (r *ServerConfig) LogStr(key string, content string) error {

	k := createConfigKey(key)
	return r.repo.Set(SERVER_CONFIG, k, []byte(content))
}

// Add Entry to server config
func (r *ServerConfig) LogInt(key string, content uint64) error {

	k := createConfigKey(key)
	return r.repo.Set(SERVER_CONFIG, k, []byte(strconv.FormatUint(content, 10)))
}

// Retrieve entry from server config
func (r *ServerConfig) GetStr(key string) (string, error) {

	k := createConfigKey(key)
	data, err := r.repo.Get(SERVER_CONFIG, k)
	if err != nil {
		if storeErr, ok := err.(*StoreError); ok && storeErr != nil {
			return "", storeErr
		}
		return "", common.WrapError(common.SERVER_CONFIG_ERROR, "Key = "+key, err)
	}

	return string(data), nil
}

// Retrieve entry from server config
func (r *ServerConfig) GetInt(key string) (uint64, error) {

	k := createConfigKey(key)
	data, err := r.repo.Get(SERVER_CONFIG, k)
	if err != nil {
		if storeErr, ok := err.(*StoreError); ok && storeErr != nil {
			return 0, storeErr
		}
		return 0, common.WrapError(common.SERVER_CONFIG_ERROR, "Key = "+key, err)
	}

	return strconv.ParseUint(string(data), 10, 64)
}

// Delete from server config
func (r *ServerConfig) Delete(key string) error {

	k := createConfigKey(key)
	return r.repo.Delete(SERVER_CONFIG, k)
}

////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func createConfigKey(key string) string {
	return key
}

func (r *ServerConfig) bootstrap() {
	value, err := r.GetInt(common.CONFIG_MAGIC)
	if err != nil || value != common.CONFIG_MAGIC_VALUE {
		r.bootstrapKey(common.CONFIG_MAGIC, uint64(common.CONFIG_MAGIC_VALUE))
		r.bootstrapKey(common.CONFIG_CURRENT_EPOCH, uint64(common.BOOTSTRAP_CURRENT_EPOCH))
		r.bootstrapKey(common.CONFIG_ACCEPTED_EPOCH, uint64(common.BOOTSTRAP_ACCEPTED_EPOCH))
		r.bootstrapKey(common.CONFIG_LAST_LOGGED_TXID, uint64(common.BOOTSTRAP_LAST_LOGGED_TXID))
		r.bootstrapKey(common.CONFIG_LAST_COMMITTED_TXID, uint64(common.BOOTSTRAP_LAST_COMMITTED_TXID))
		if err := r.repo.Commit(); err != nil {
			panic(fmt.Sprintf("unable to initialize gometa due to storage error = %v", err))
		}
	}
}

// Add Entry to server config
func (r *ServerConfig) bootstrapKey(key string, content uint64) {

	k := createConfigKey(key)
	if err := r.repo.SetNoCommit(SERVER_CONFIG, k, []byte(strconv.FormatUint(content, 10))); err != nil {
		panic(fmt.Sprintf("unable to initialize gometa due to storage error = %v", err))
	}
}
