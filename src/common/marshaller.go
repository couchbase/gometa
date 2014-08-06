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
	totalLen := tyLen + msgLen

	payload := make([]byte, 16+totalLen)
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
