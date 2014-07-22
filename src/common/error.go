package common

import (
)

type ErrorCode byte
const (
	PROTOCOL_ERROR ErrorCode = iota  	
	SERVER_ERROR 
	SERVER_CONFIG_ERROR
)

type Error struct {
	code		ErrorCode
	reason		string	
	cause       error  
}

func NewError(code ErrorCode, reason string) *Error {
	return &Error{code : code, reason : reason, cause : nil}
} 

func WrapError(code ErrorCode, reason string, cause *Error) *Error {
	return &Error{code : code, reason : reason, cause : cause}
} 

func (e *Error) Error() string {
	return codeToStr(e.code) + " : " + e.reason
}

func codeToStr(code ErrorCode) string {
	switch code {
		case PROTOCOL_ERROR : return "Protocol Error"
		case SERVER_ERROR : return "Server Error"
		case SERVER_CONFIG_ERROR : return "Server Config Error"
	}
	
    return "Undefined Error"
}