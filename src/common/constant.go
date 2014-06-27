package common 

import (
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////////////////////////

var MAX_PARTICIPANTS = 50            				// maximum number of participants 
var MAX_FOLLOWERS = 100              				// maximum number of followers 
var MAX_PEERS = 150                  				// maximum number of peers 
var MAX_PROPOSALS = 1000             				// maximum number of proposals 
var MAX_DATAGRAM_SIZE  = 1000        				// maximum size of datagram
var MESSAGE_PORT = 9999              				// port for receving message from peer (e.g. request/proposal)
var ELECTION_PORT = 9998             				// port for receving election votes from peer 
var MESSAGE_TRANSPORT_TYPE = "tcp"   				// network protocol for message transport 
var ELECTION_TRANSPORT_TYPE = "udp"  				// network protocol for election vote transport 
var BALLOT_TIMEOUT time.Duration = 100     		    // timeout for a ballot (millisecond)
var BALLOT_MAX_TIMEOUT time.Duration = 5000         // timeout for a ballot (millisecond)
var REPOSITORY_NAME = "MetadataStore"       		// Forest db name for metadata store
var PREFIX_COMMIT_LOG_PATH = "/cstore/commitlog"   	// Directory prefix for commit log 