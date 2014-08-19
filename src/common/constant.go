package common

import (
	"time"
	"math"
)

/////////////////////////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////////////////////////

var MAX_PARTICIPANTS = 50                                            // maximum number of participants
var MAX_FOLLOWERS = 100                                              // maximum number of followers
var MAX_PEERS = 150                                                  // maximum number of peers
var MAX_PROPOSALS = 1000                                             // maximum number of proposals
var MAX_DATAGRAM_SIZE = 1000                                         // maximum size of datagram
var MESSAGE_PORT = 9999                                              // port for receving message from peer (e.g. request/proposal)
var ELECTION_PORT = 9998                                             // port for receving election votes from peer
var MESSAGE_TRANSPORT_TYPE = "tcp"                                   // network protocol for message transport
var ELECTION_TRANSPORT_TYPE = "udp"                                  // network protocol for election vote transport
var BALLOT_TIMEOUT time.Duration = 50                                // timeout for a ballot (millisecond)
var BALLOT_MAX_TIMEOUT time.Duration = 500                           // max timeout for a ballot (millisecond)
var SYNC_TIMEOUT time.Duration = 10000                               // timeout for synchronization (millisecond)
var LEADER_TIMEOUT time.Duration = 100000                            // timeout for leader (millisecond)
var REPOSITORY_NAME = "MetadataStore"                                // Forest db name for metadata store
var PREFIX_SERVER_CONFIG_PATH = "/couchbase/cstore/1/server/config/" // Directory prefix for server config
var PREFIX_COMMIT_LOG_PATH = "/couchbase/cstore/100/commitlog/"      // Directory prefix for commit log
var PREFIX_DATA_PATH = "/couchbase/cstore/200/data/"                 // Directory prefix for user data 
var CONFIG_ACCEPTED_EPOCH = "AcceptedEpoch"                          // Server Config Param : AcceptedEpoch
var CONFIG_CURRENT_EPOCH = "CurrentEpoch"                            // Server Config Param : CurrentEpoch
var CONFIG_LAST_LOGGED_TXID = "LastLoggedTxid"                       // Server Config Param : LastLoggedTxid
var CONFIG_LAST_COMMITTED_TXID = "LastCommittedTxid"                 // Server Config Param : LastCommittedTxid
var CONFIG_MAGIC = "MagicNumber"                                     // Server Config Param : Magic Number 
var CONFIG_MAGIC_VALUE uint64 = 0x0123456789                         // Server Config Param : Magic Number Value
var MAX_EPOCH uint32 = math.MaxUint32                                // Max value for epoch 
var MAX_COUNTER uint32 = math.MaxUint32                              // Max value for counter 
var BOOTSTRAP_LAST_COMMITTED_TXID Txnid = Txnid(0)                   // Boostrap value of last committed txid
var BOOTSTRAP_LAST_LOGGED_TXID Txnid = Txnid(0)                      // Boostrap value of last logged txid
var BOOTSTRAP_CURRENT_EPOCH uint32 = 0                      		 // Boostrap value of current epoch 
var BOOTSTRAP_ACCEPTED_EPOCH uint32 = 0                      		 // Boostrap value of accepted epoch 

