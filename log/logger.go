package log

// Logger interface
type Logger interface {
	// Warnings, logged by default.
	Warnf(format string, v ...interface{})
	// Errors, logged by default.
	Errorf(format string, v ...interface{})
	// Fatal errors. Will not terminate execution.
	Fatalf(format string, v ...interface{})
	// Informational messages.
	Infof(format string, v ...interface{})
	// Debugging messages. Not logged by default
	Debugf(format string, v ...interface{})
	// Program execution tracing. Not logged by default
	Tracef(format string, v ...interface{})
	// Call and print result only if debug enabled
	LazyDebug(fn func() string)
	// Call and print result only if trace enabled
	LazyTrace(fn func() string)
	// Print stack trace at specified log level
	StackTrace() string
}

// Global logger
var Current Logger = nil