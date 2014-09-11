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
	return OPCODE_INVALID
}

func GetOpCodeFromInt(i uint32) OpCode {
	return OpCode(i)
}
