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

type ErrorCode byte

const (
	PROTOCOL_ERROR ErrorCode = iota
	SERVER_ERROR
	SERVER_CONFIG_ERROR
	FATAL_ERROR
	ARG_ERROR
	ELECTION_ERROR
	CLIENT_ERROR
	REPO_ERROR
)

type Error struct {
	code   ErrorCode
	reason string
	cause  error
}

func NewError(code ErrorCode, reason string) *Error {
	return &Error{code: code, reason: reason, cause: nil}
}

func WrapError(code ErrorCode, reason string, cause error) *Error {
	return &Error{code: code, reason: reason, cause: cause}
}

func (e *Error) IsFatal() bool {
	return e.code == FATAL_ERROR
}

func (e *Error) Error() string {
	return codeToStr(e.code) + " : " + e.reason + " : " + e.cause.Error()
}

func codeToStr(code ErrorCode) string {
	switch code {
	case PROTOCOL_ERROR:
		return "Protocol Error"
	case SERVER_ERROR:
		return "Server Error"
	case SERVER_CONFIG_ERROR:
		return "Server Config Error"
	}

	return "Undefined Error"
}

func Debug() bool {
	return false
}
