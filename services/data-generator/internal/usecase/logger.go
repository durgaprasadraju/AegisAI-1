package usecase

import "fmt"

// ============================================================================
// LOGGER INTERFACE - Simple Logging Abstraction
// ============================================================================
// Logger defines a simple logging interface for use cases.
// This keeps the use case layer independent of specific logging libraries.
//
// Why an interface?
// - Use cases don't need to know about logrus, zap, or other libraries
// - Easy to mock for testing
// - Can swap implementations without changing use case code
// - Follows dependency inversion principle
type Logger interface {
	// Info logs an informational message
	Info(msg string)

	// Error logs an error message
	Error(msg string, err error)

	// Debug logs a debug message (optional, may be no-op in production)
	Debug(msg string)
}

// ============================================================================
// SIMPLE LOGGER IMPLEMENTATION
// ============================================================================
// SimpleLogger is a basic logger implementation that writes to stdout.
// This is suitable for development and can be replaced with a more
// sophisticated logger in production without changing use case code.

// SimpleLogger implements Logger interface with fmt.Printf
type SimpleLogger struct{}

// NewSimpleLogger creates a new simple logger
func NewSimpleLogger() *SimpleLogger {
	return &SimpleLogger{}
}

// Info logs an informational message
func (l *SimpleLogger) Info(msg string) {
	fmt.Printf("[INFO] %s\n", msg)
}

// Error logs an error message
func (l *SimpleLogger) Error(msg string, err error) {
	if err != nil {
		fmt.Printf("[ERROR] %s: %v\n", msg, err)
	} else {
		fmt.Printf("[ERROR] %s\n", msg)
	}
}

// Debug logs a debug message
func (l *SimpleLogger) Debug(msg string) {
	fmt.Printf("[DEBUG] %s\n", msg)
}
