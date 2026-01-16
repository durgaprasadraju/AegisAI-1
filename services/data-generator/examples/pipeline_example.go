//go:build pipeline_example
// +build pipeline_example

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
	"github.com/aegisai/data-generator/internal/usecase"
)

// ============================================================================
// PIPELINE EXAMPLE - Demonstrates Concurrent Ingestion
// ============================================================================
// This example demonstrates:
// 1. Creating a concurrent ingestion pipeline
// 2. Generating and submitting 20 sensor events
// 3. Graceful shutdown handling
// 4. Real-world usage patterns
//
// To run this example:
//   cd services/data-generator
//   go run -tags pipeline_example ./examples/pipeline_example.go

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down pipeline...")
		cancel()
	}()

	// Run the pipeline example
	runPipelineExample(ctx)
}

// runPipelineExample demonstrates the concurrent ingestion pipeline
func runPipelineExample(ctx context.Context) {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  AegisAI Concurrent Ingestion Pipeline (Day 5)             ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ============================================================================
	// STEP 1: Setup Dependencies
	// ============================================================================
	fmt.Println("=== Step 1: Setup Dependencies ===")

	// Create logger
	logger := usecase.NewSimpleLogger()

	// Create mock ingester (for demonstration - in production, use real adapter)
	ingester := NewMockIngester(logger)

	// Load configuration from environment
	config := usecase.LoadPipelineConfig()
	fmt.Printf("Pipeline Config:\n")
	fmt.Printf("  - Worker Count: %d\n", config.WorkerCount)
	fmt.Printf("  - Channel Buffer Size: %d\n", config.ChannelBufferSize)
	fmt.Println()

	// ============================================================================
	// STEP 2: Create Use Cases
	// ============================================================================
	fmt.Println("=== Step 2: Create Use Cases ===")

	// Create single event use case
	singleUseCase := usecase.NewIngestSensorUseCase(ingester, logger)

	// Create pipeline use case
	pipeline := usecase.NewIngestPipelineUseCase(config, singleUseCase, logger)
	fmt.Println("Use cases created")
	fmt.Println()

	// ============================================================================
	// STEP 3: Start Pipeline
	// ============================================================================
	fmt.Println("=== Step 3: Start Pipeline ===")

	if err := pipeline.Start(ctx); err != nil {
		fmt.Printf("Error starting pipeline: %v\n", err)
		return
	}
	fmt.Println("Pipeline started, ready to accept events")
	fmt.Println()

	// ============================================================================
	// STEP 4: Generate and Submit Events
	// ============================================================================
	fmt.Println("=== Step 4: Generate and Submit 20 Events ===")

	eventCount := 20
	for i := 0; i < eventCount; i++ {
		event := generatePipelineSensorEvent(i + 1)
		if err := pipeline.Ingestion(event); err != nil {
			fmt.Printf("Error submitting event %d: %v\n", i+1, err)
		} else {
			fmt.Printf("  ✓ Submitted event %d: %s (sensor: %s)\n", i+1, event.EventID, event.SensorID)
		}
		// Small delay to simulate real-world event arrival
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println()

	// ============================================================================
	// STEP 5: Wait for Processing
	// ============================================================================
	fmt.Println("=== Step 5: Wait for Events to Process ===")
	fmt.Println("Waiting for pipeline to process all events...")

	// Give workers time to process
	time.Sleep(2 * time.Second)

	// ============================================================================
	// STEP 6: Graceful Shutdown
	// ============================================================================
	fmt.Println("=== Step 6: Graceful Shutdown ===")
	pipeline.Shutdown()

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    Pipeline Example Complete                   ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Key Takeaways:")
	fmt.Println("  • Pipeline processed events concurrently using worker pool")
	fmt.Println("  • Events were queued in buffered channel")
	fmt.Println("  • Workers processed events independently")
	fmt.Println("  • Graceful shutdown ensured no events were lost")
	fmt.Println()
}

// generatePipelineSensorEvent creates a sample sensor event for demonstration
func generatePipelineSensorEvent(index int) domain.SensorEvent {
	sensorTypes := []string{"temperature", "humidity", "pressure", "moisture"}
	sensorType := sensorTypes[index%len(sensorTypes)]

	return domain.SensorEvent{
		EventID:    fmt.Sprintf("event-%03d", index),
		SensorID:   fmt.Sprintf("sensor-%03d", index),
		SensorType: sensorType,
		Value:      float64(20 + index),
		Unit:       getPipelineUnitForType(sensorType),
		Timestamp:  time.Now().Add(time.Duration(index) * time.Second),
		Tags: map[string]string{
			"location": "warehouse-A",
			"zone":     fmt.Sprintf("zone-%d", (index%3)+1),
			"source":   "pipeline-example",
		},
	}
}

// getPipelineUnitForType returns the appropriate unit for a sensor type
func getPipelineUnitForType(sensorType string) string {
	switch sensorType {
	case "temperature":
		return "celsius"
	case "humidity", "moisture":
		return "percent"
	case "pressure":
		return "pascal"
	default:
		return "unknown"
	}
}

// ============================================================================
// MOCK INGESTER - For Demonstration Only
// ============================================================================
// In production, this would be replaced with a real adapter that:
// - Publishes to Kafka
// - Saves to DynamoDB
// - Sends to REST API
// etc.

// MockIngester is a simple mock implementation for demonstration
type MockIngester struct {
	logger usecase.Logger
}

// NewMockIngester creates a new mock ingester
func NewMockIngester(logger usecase.Logger) *MockIngester {
	return &MockIngester{logger: logger}
}

// Ingest simulates ingestion by logging the event
func (m *MockIngester) Ingest(ctx context.Context, event domain.SensorEvent) error {
	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// In production, this would:
	// - Publish to Kafka topic
	// - Save to DynamoDB
	// - Send HTTP request
	// etc.

	m.logger.Debug(fmt.Sprintf("Mock ingester: processed event %s", event.EventID))
	return nil
}
