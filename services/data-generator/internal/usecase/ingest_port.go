package usecase

import (
	"context"

	"github.com/aegisai/data-generator/internal/domain"
)

// ============================================================================
// PORT INTERFACE - Clean Architecture Boundary
// ============================================================================
// SensorIngester is a port (interface) that defines the contract for
// ingesting sensor events. This follows Clean Architecture principles:
// - Use cases depend on ports (interfaces), not concrete implementations
// - Implementations (adapters) live in infrastructure layer
// - Allows swapping implementations without changing use case logic
//
// In Clean Architecture:
// - Port = Interface (what we need)
// - Adapter = Implementation (how we do it)
// - Use case depends on port, adapter implements port
//
// This separation allows:
// - Testing with mock implementations
// - Multiple implementations (Kafka, SQS, HTTP, etc.)
// - Gradual migration between implementations
type SensorIngester interface {
	// Ingest processes a single sensor event.
	// This method is called by the use case to persist/publish the event.
	// The implementation (adapter) decides how to handle it:
	// - Kafka adapter: publish to Kafka topic
	// - DynamoDB adapter: save to database
	// - HTTP adapter: send to REST API
	// - Mock adapter: capture for testing
	//
	// Context is used for:
	// - Cancellation propagation
	// - Timeout handling
	// - Request-scoped values
	Ingest(ctx context.Context, event domain.SensorEvent) error
}
