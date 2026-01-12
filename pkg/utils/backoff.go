package utils

import (
	"time"
)

// BackoffStrategy defines backoff calculation
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

// ExponentialBackoff implements exponential backoff
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// Next calculates the next backoff duration
func (b *ExponentialBackoff) Next(attempt int) time.Duration {
	// TODO: Implement exponential backoff calculation
	return b.InitialDelay
}
