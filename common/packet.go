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
