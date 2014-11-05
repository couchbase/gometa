// @author Couchbase <info@couchbase.com>
// @copyright 2014 NorthScale, Inc.
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
//
package common

import (
	"encoding/binary"
)

/////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////

//
// Take a Packet and create a serialized byte array.
//  8 bytes : total length of msg (excluding size of total length itself)
//  8 bytes : length of the message type (protobuf struct)
//  n bytes : message type (protobuf struct)
//  n bytes : message content  (protobuf)
//
func Marshall(packet Packet) ([]byte, error) {

	typeOfMsg := FindPacketConcreteType(packet).Name()
	msg, err := packet.Encode()
	if err != nil {
		return nil, err
	}

	tyLen := len(typeOfMsg)
	msgLen := len(msg)
	totalLen := tyLen + msgLen + 8

	payload := make([]byte, totalLen+8)
	binary.BigEndian.PutUint64(payload, uint64(totalLen))
	binary.BigEndian.PutUint64(payload[8:], uint64(tyLen))
	copy(payload[16:], typeOfMsg)
	copy(payload[16+tyLen:], msg)

	return payload, nil
}

//
// Take a byte stream and create a Packet
//
func UnMarshall(payload []byte) (Packet, error) {

	tyLen := binary.BigEndian.Uint64(payload[:8])
	payload = payload[8:]
	typeOfMsg := string(payload[:tyLen])

	packet, err := CreatePacketByName(typeOfMsg)
	if err != nil {
		return nil, err
	}

	payload = payload[tyLen:]
	if err := packet.Decode(payload); err != nil {
		return nil, err
	}

	return packet, nil
}
