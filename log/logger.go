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

package log

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"fmt"
)

//
// This file is based on indexing/secondary/common/debug.go
// At some point in time, it will need to move to a common
// logging facility.
//

// Error, Warning, Fatal are always logged
const (
	// LogLevelInfo log messages for info
	LogLevelInfo int = iota + 1
	// LogLevelDebug log messages for info and debug
	LogLevelDebug
	// LogLevelTrace log messages info, debug and trace
	LogLevelTrace
)

var logLevel int
var logFile io.Writer = os.Stdout
var logger *log.Logger
var prefix string = ""

// Logger interface for sub-components to do logging.
type Logger interface {
	// Warnf to log message and warning messages will be logged.
	Warnf(format string, v ...interface{})

	// Errorf to log message and warning messages will be logged.
	Errorf(format string, v ...interface{})

	// Fatalf to log message and warning messages will be logged.
	Fatalf(format string, v ...interface{})

	// Infof to log message at info level.
	Infof(format string, v ...interface{})

	// Debugf to log message at info level.
	Debugf(format string, v ...interface{})

	// Tracef to log message at info level.
	Tracef(format string, v ...interface{})

	// StackTrace parse string `s` and log it as error message.
	StackTrace(s string)
}

func init() {
	logger = log.New(logFile, "", log.Lmicroseconds)
}

// LogLevel returns current log level
func LogLevel() int {
	return logLevel
}

// LogIgnore to ignore all log messages.
func LogIgnore() {
	logger = log.New(ioutil.Discard, "", log.Lmicroseconds)
}

// LogEnable to enable / re-enable log output.
func LogEnable() {
	logger = log.New(logFile, "", log.Lmicroseconds)
}

// SetLogLevel sets current log level
func SetLogLevel(level int) {
	logLevel = level
}

// SetLogWriter sets output file for log messages
func SetLogWriter(w io.Writer) {
	logger = log.New(w, "", log.Lmicroseconds)
	logFile = w
}

//-------------------------------
// Warning, Error, Fatal messages
//-------------------------------

func print(severity string, format string, v ...interface{}) {
	if len(prefix) == 0 {
		logger.Printf(fmt.Sprintf("[%v ] ", severity) + format, v...)
	} else {
		logger.Printf(fmt.Sprintf("[%v ] %v: ", severity, prefix) + format, v...)
	}
}

// Warnf similar to fmt.Printf
func Warnf(format string, v ...interface{}) {
	print("WARN", format, v...)
}

// Errorf similar to fmt.Printf
func Errorf(format string, v ...interface{}) {
	print("ERROR", format, v...)
}

// Fatalf similar to fmt.Fatalf
func Fatalf(format string, v ...interface{}) {
	print("FATAL", format, v...)
}

//------------------------
// Informational debugging
//------------------------

// Infof if logLevel >= Info
func Infof(format string, v ...interface{}) {
	if logLevel >= LogLevelInfo {
		print("INFO", format, v...)
	}
}

//----------------
// Basic debugging
//----------------

// Debugf if logLevel >= Debug
func Debugf(format string, v ...interface{}) {
	if logLevel >= LogLevelDebug {
		print("DEBUG", format, v...)
	}
}

//----------------------
// Trace-level debugging
//----------------------

// Tracef if logLevel >= Trace
func Tracef(format string, v ...interface{}) {
	if logLevel >= LogLevelTrace {
		print("TRACE", format, v...)
	}
}

// StackTrace formats the output of debug.Stack()
func StackTrace(s string) {
	for _, line := range strings.Split(s, "\n") {
		Errorf("%s\n", line)
	}
}

//---------------------------
// log compatible public API 
//---------------------------

// Prefix returns the output prefix for the standard logger.
func Prefix() string {
	return prefix
}

// SetPrefix sets the output prefix for the standard logger.
func SetPrefix(p string) {
	prefix = p 
}

// Print calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Print.
func Print(v ...interface{}) {
    Tracef("%v", v)
}

// Printf calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Printf.
func Printf(format string, v ...interface{}) {
    Tracef(format, v...)
}

// Println calls Output to print to the standard logger.
// Arguments are handled in the manner of fmt.Println.
func Println(v ...interface{}) {
    Tracef("%v", v...)
}
