package common

import (
)

type ErrorCode byte
const (
	PROTOCOL_ERROR ErrorCode = iota  	
)

type Error struct {
	code		ErrorCode
	reason		string	
}

func NewError(code ErrorCode, reason string) *Error {
	return &Error{code : code, reason : reason}
} 

func (e *Error) Error() string {
	return codeToStr(e.code) + " : " + e.reason
}

func codeToStr(code ErrorCode) string {
	switch code {
		case PROTOCOL_ERROR : return "Protocol Error"
	}
	
    return "Undefined Error"
}