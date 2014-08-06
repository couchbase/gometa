package common

import (
	"reflect"
)

var gRegistry = NewPacketRegistry()

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

type Packet interface {
	// Name of the message
	Name() string

	// Encode function shall marshal message to byte array.
	Encode() (data []byte, err error)

	// Decode function shall unmarshal byte array back to message.
	Decode(data []byte) (err error)
	
	// Debug Print
	Print()
}

type PacketRegistry struct {
	mapping map[string]Packet
}

/////////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////////

func NewPacketRegistry() *PacketRegistry {
	return &PacketRegistry{mapping: make(map[string]Packet)}
}

func RegisterPacketByName(name string, instance Packet) {
	gRegistry.mapping[name] = instance
}

func CreatePacketByName(name string) (Packet, error) {
	sample := gRegistry.mapping[name]
	if sample == nil {
		// TODO: return actual error
		return nil, nil
	}

	return reflect.New(FindPacketConcreteType(sample)).Interface().(Packet), nil
}

func FindPacketConcreteType(packet Packet) reflect.Type {
	return reflect.ValueOf(packet).Elem().Type()
}
