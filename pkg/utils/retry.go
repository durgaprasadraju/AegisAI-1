package utils

import (
	"time"
)

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxAttempts int
	Delay       time.Duration
}

// Retry executes a function with retry logic
func Retry(fn func() error, config RetryConfig) error {
	// TODO: Implement retry logic
	return nil
}
