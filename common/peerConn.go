// @author Couchbase <info@couchbase.com>
// @copyright 2022 Couchbase, Inc.
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

package common

import (
	"net"
)

/////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////

//
// PeerConn encapsulates TCP/TLS connection, its corresponding PeerPipe and
// the first non-auth packet received by the auth function, if any.
//
type PeerConn struct {
	conn   net.Conn
	pipe   *PeerPipe
	packet Packet
}

func NewPeerConn(conn net.Conn, pipe *PeerPipe, packet Packet) *PeerConn {
	return &PeerConn{
		conn:   conn,
		pipe:   pipe,
		packet: packet,
	}
}

func (peerConn *PeerConn) GetConn() net.Conn {
	return peerConn.conn
}

func (peerConn *PeerConn) GetPeerPipe() *PeerPipe {
	return peerConn.pipe
}

func (peerConn *PeerConn) GetPacket() Packet {
	return peerConn.packet
}
