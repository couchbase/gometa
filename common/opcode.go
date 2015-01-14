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

package common

import ()

/////////////////////////////////////////////////////////////////////////////
// OpCode
/////////////////////////////////////////////////////////////////////////////

type OpCode byte

const (
	OPCODE_INVALID OpCode = iota
	OPCODE_ADD
	OPCODE_SET
	OPCODE_DELETE
	OPCODE_GET
	OPCODE_ABORT
	OPCODE_CUSTOM_ADD
	OPCODE_CUSTOM_SET
	OPCODE_CUSTOM_DELETE
	OPCODE_RESPONSE
	OPCODE_STREAM_BEGIN_MARKER
	OPCODE_STREAM_END_MARKER
)

func GetOpCodeStr(r OpCode) string {
	switch r {
	case OPCODE_ADD:
		return "Add"
	case OPCODE_SET:
		return "Set"
	case OPCODE_DELETE:
		return "Delete"
	case OPCODE_GET:
		return "Get"
	case OPCODE_STREAM_BEGIN_MARKER:
		return "StreamBegin"
	case OPCODE_STREAM_END_MARKER:
		return "StreamEnd"
	case OPCODE_ABORT:
		return "Abort"
	case OPCODE_CUSTOM_ADD:
		return "Custom Add"
	case OPCODE_CUSTOM_SET:
		return "Custom Set"
	case OPCODE_CUSTOM_DELETE:
		return "Custom Delete"
	case OPCODE_RESPONSE:
		return "Response"
	default:
		return "Invalid"
	}
}

func GetOpCode(s string) OpCode {
	if s == "Add" {
		return OPCODE_ADD
	}
	if s == "Set" {
		return OPCODE_SET
	}
	if s == "Delete" {
		return OPCODE_DELETE
	}
	if s == "Get" {
		return OPCODE_GET
	}
	if s == "StreamBegin" {
		return OPCODE_STREAM_BEGIN_MARKER
	}
	if s == "StreamEnd" {
		return OPCODE_STREAM_END_MARKER
	}
	if s == "Abort" {
		return OPCODE_ABORT
	}
	if s == "Custom Add" {
		return OPCODE_CUSTOM_ADD
	}
	if s == "Custom Set" {
		return OPCODE_CUSTOM_SET
	}
	if s == "Custom Delete" {
		return OPCODE_CUSTOM_DELETE
	}
	if s == "Response" {
		return OPCODE_RESPONSE
	}
	return OPCODE_INVALID
}

func GetOpCodeFromInt(i uint32) OpCode {
	return OpCode(i)
}

func IsCustomOpCode(opCode OpCode) bool {
	return opCode == OPCODE_CUSTOM_ADD || opCode == OPCODE_CUSTOM_SET || opCode == OPCODE_CUSTOM_DELETE
}
