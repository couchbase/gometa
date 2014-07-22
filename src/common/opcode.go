package common

import (

)

/////////////////////////////////////////////////////////////////////////////
// OpCode 
/////////////////////////////////////////////////////////////////////////////

type OpCode byte
const (
	OPCODE_INVALID OpCode = iota 
	OPCODE_ADD 
	OPCODE_SET
	OPCODE_DELETE
)

func GetOpCodeStr(r OpCode) string {
	switch r {
		case OPCODE_ADD: return "Add" 
		case OPCODE_SET: return "Set" 
		case OPCODE_DELETE: return "Delete" 
		default : return "Invalid"
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
	return OPCODE_INVALID 
}

func GetOpCodeFromInt(i uint32) OpCode {
	return OpCode(i)
}