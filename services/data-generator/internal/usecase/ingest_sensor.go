package usecase

import (
	"context"
	"fmt"

	"github.com/aegisai/data-generator/internal/domain"
)

// ============================================================================
// SINGLE EVENT INGESTION USE CASE
// ============================================================================
// IngestSensorUseCase handles the business logic for ingesting a single
// sensor event. This is the unit of work that each pipeline worker executes.
//
// Responsibilities:
// - Validate the event (basic checks)
// - Call the SensorIngester port to persist/publish the event
// - Handle errors gracefully (log, don't crash)
//
// This use case is pure business logic - no infrastructure dependencies.
// It depends on:
// - domain.SensorEvent (domain model)
// - SensorIngester (port interface)
// - Logger (simple interface)
//
// It does NOT depend on:
// - Kafka, DynamoDB, AWS SDK
// - Any infrastructure packages

// IngestSensorUseCase handles ingestion of a single sensor event
type IngestSensorUseCase struct {
	ingester SensorIngester // Port interface - depends on abstraction
	logger   Logger         // Logger interface - simple abstraction
}

// NewIngestSensorUseCase creates a new single event ingestion use case
func NewIngestSensorUseCase(ingester SensorIngester, logger Logger) *IngestSensorUseCase {
	return &IngestSensorUseCase{
		ingester: ingester,
		logger:   logger,
	}
}

// Execute processes a single sensor event.
// This method is called by pipeline workers for each event.
//
// Error handling strategy:
// - Validation errors: log and return error (event is invalid)
// - Ingestion errors: log and return error (infrastructure issue)
// - Errors are returned to caller, but pipeline continues processing
//
// Context usage:
// - Used for cancellation propagation
// - Passed to ingester for timeout handling
func (uc *IngestSensorUseCase) Execute(ctx context.Context, event domain.SensorEvent) error {
	// Basic validation
	if err := uc.validateEvent(event); err != nil {
		uc.logger.Error(fmt.Sprintf("Invalid event: %s", event.EventID), err)
		return fmt.Errorf("validation failed: %w", err)
	}

	// Check context cancellation before processing
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue processing
	}

	// Call the ingester port (adapter will handle actual persistence/publishing)
	if err := uc.ingester.Ingest(ctx, event); err != nil {
		uc.logger.Error(fmt.Sprintf("Failed to ingest event: %s", event.EventID), err)
		return fmt.Errorf("ingestion failed: %w", err)
	}

	uc.logger.Debug(fmt.Sprintf("Successfully ingested event: %s (sensor: %s)", event.EventID, event.SensorID))
	return nil
}

// validateEvent performs basic validation on the sensor event
func (uc *IngestSensorUseCase) validateEvent(event domain.SensorEvent) error {
	if event.EventID == "" {
		return fmt.Errorf("event ID is required")
	}
	if event.SensorID == "" {
		return fmt.Errorf("sensor ID is required")
	}
	if event.SensorType == "" {
		return fmt.Errorf("sensor type is required")
	}
	if event.Unit == "" {
		return fmt.Errorf("unit is required")
	}
	if event.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	// Value can be any float64 (including negative for temperature)
	return nil
}
