//go:build consumer_group_example
// +build consumer_group_example

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
	"github.com/aegisai/data-generator/internal/usecase"
)

// ============================================================================
// CONSUMER GROUP EXAMPLE - Demonstrates Kafka-like Stream Processing
// ============================================================================
// This example demonstrates:
// 1. Partition-based routing (same SensorID → same partition)
// 2. Concurrent processing across partitions
// 3. Retry mechanism with exponential backoff
// 4. Dead Letter Queue for failed events
// 5. Backpressure handling
//
// To run this example:
//   cd services/data-generator
//   go run -tags consumer_group_example ./examples/consumer_group_example.go

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down consumer group...")
		cancel()
	}()

	// Run the consumer group example
	runConsumerGroupExample(ctx)
}

// runConsumerGroupExample demonstrates the Kafka-like consumer group
func runConsumerGroupExample(ctx context.Context) {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  AegisAI Kafka Consumer Group Simulation (Day 6)           ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ============================================================================
	// STEP 1: Setup Dependencies
	// ============================================================================
	fmt.Println("=== Step 1: Setup Dependencies ===")

	// Create logger
	logger := usecase.NewSimpleLogger()

	// Create mock ingester with failure simulation
	// This will randomly fail some events to demonstrate retry and DLQ
	failureRate := 0.15 // 15% failure rate for demonstration
	ingester := NewFailingMockIngester(logger, failureRate)

	// Load consumer group configuration
	config := usecase.LoadConsumerGroupConfig()
	fmt.Printf("Consumer Group Config:\n")
	fmt.Printf("  - Partitions: %d\n", config.PartitionCount)
	fmt.Printf("  - Workers per Partition: %d\n", config.WorkersPerPartition)
	fmt.Printf("  - Total Workers: %d\n", config.PartitionCount*config.WorkersPerPartition)
	fmt.Printf("  - Max Retries: %d\n", config.MaxRetries)
	fmt.Printf("  - DLQ Buffer Size: %d\n", config.DLQBufferSize)
	fmt.Printf("  - Backpressure Mode: %s\n", config.BackpressureMode)
	fmt.Println()

	// ============================================================================
	// STEP 2: Create Use Cases
	// ============================================================================
	fmt.Println("=== Step 2: Create Use Cases ===")

	// Create single event use case
	singleUseCase := usecase.NewIngestSensorUseCase(ingester, logger)

	// Create consumer group use case
	consumerGroup := usecase.NewConsumerGroupUseCase(config, singleUseCase, logger)
	fmt.Println("Use cases created")
	fmt.Println()

	// ============================================================================
	// STEP 3: Start Consumer Group
	// ============================================================================
	fmt.Println("=== Step 3: Start Consumer Group ===")

	if err := consumerGroup.Start(ctx); err != nil {
		fmt.Printf("Error starting consumer group: %v\n", err)
		return
	}
	fmt.Println("Consumer group started, ready to accept events")
	fmt.Println()

	// ============================================================================
	// STEP 4: Generate and Route Events
	// ============================================================================
	fmt.Println("=== Step 4: Generate and Route 30 Events ===")
	fmt.Println("Demonstrating partition routing (same SensorID → same partition)...")

	eventCount := 30
	partitionMap := make(map[string]int) // Track which partition each SensorID goes to

	// Generate events with some duplicate SensorIDs to show partition routing
	sensorIDs := []string{
		"sensor-001", "sensor-002", "sensor-003", // Will go to different partitions
		"sensor-001", "sensor-002", "sensor-001", // Duplicates to show same partition
		"sensor-004", "sensor-005", "sensor-006",
		"sensor-002", "sensor-003", "sensor-001", // More duplicates
		"sensor-007", "sensor-008", "sensor-009",
		"sensor-001", "sensor-002", "sensor-010", // More routing examples
		"sensor-011", "sensor-012", "sensor-013",
		"sensor-001", "sensor-002", "sensor-014", // Demonstrating consistent routing
		"sensor-015", "sensor-016", "sensor-017",
		"sensor-001", "sensor-002", "sensor-018", // Final duplicates
	}

	for i := 0; i < eventCount && i < len(sensorIDs); i++ {
		event := generateSensorEvent(i+1, sensorIDs[i])
		partitionID := routeToPartition(event, config.PartitionCount)

		// Track partition assignment
		if existingPartition, exists := partitionMap[event.SensorID]; exists {
			if existingPartition != partitionID {
				fmt.Printf("  ⚠️  ERROR: SensorID %s routed to different partitions! (%d vs %d)\n",
					event.SensorID, existingPartition, partitionID)
			} else {
				fmt.Printf("  ✓ Event %d: %s → Partition %d (consistent routing)\n",
					i+1, event.SensorID, partitionID)
			}
		} else {
			partitionMap[event.SensorID] = partitionID
			fmt.Printf("  → Event %d: %s → Partition %d\n", i+1, event.SensorID, partitionID)
		}

		// Submit event to consumer group
		if err := consumerGroup.Ingest(event); err != nil {
			fmt.Printf("  ✗ Error submitting event %d: %v\n", i+1, err)
		}

		// Small delay to simulate real-world event arrival
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println()

	// ============================================================================
	// STEP 5: Demonstrate Partition Routing Consistency
	// ============================================================================
	fmt.Println("=== Step 5: Partition Routing Consistency ===")
	fmt.Println("Verifying that same SensorID always routes to same partition:")
	for sensorID, partitionID := range partitionMap {
		fmt.Printf("  %s → Partition %d\n", sensorID, partitionID)
	}
	fmt.Println()

	// ============================================================================
	// STEP 6: Wait for Processing
	// ============================================================================
	fmt.Println("=== Step 6: Wait for Events to Process ===")
	fmt.Println("Processing events with retries and DLQ...")
	fmt.Println("(Some events will fail randomly to demonstrate retry mechanism)")
	fmt.Println()

	// Give workers time to process, retry, and send to DLQ
	time.Sleep(5 * time.Second)

	// ============================================================================
	// STEP 7: Graceful Shutdown
	// ============================================================================
	fmt.Println("=== Step 7: Graceful Shutdown ===")
	consumerGroup.Shutdown()

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              Consumer Group Example Complete                    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Key Takeaways:")
	fmt.Println("  • Events routed to partitions based on SensorID hash")
	fmt.Println("  • Same SensorID always goes to same partition (ordering guarantee)")
	fmt.Println("  • Events processed concurrently across partitions")
	fmt.Println("  • Failed events retried with exponential backoff")
	fmt.Println("  • Events that exceed max retries sent to DLQ")
	fmt.Println("  • Backpressure handled based on configuration mode")
	fmt.Println()
	fmt.Println("This mirrors Kafka Consumer Group behavior:")
	fmt.Println("  • Partitions = Kafka topic partitions")
	fmt.Println("  • Workers = Consumer group members")
	fmt.Println("  • Ordering = Per partition (same key → same partition)")
	fmt.Println("  • Retry = Exponential backoff (standard pattern)")
	fmt.Println("  • DLQ = Dead Letter Queue topic")
	fmt.Println()
}

// generateSensorEvent creates a sample sensor event
func generateSensorEvent(index int, sensorID string) domain.SensorEvent {
	sensorTypes := []string{"temperature", "humidity", "pressure", "moisture"}
	sensorType := sensorTypes[index%len(sensorTypes)]

	return domain.SensorEvent{
		EventID:    fmt.Sprintf("event-%03d", index),
		SensorID:   sensorID,
		SensorType: sensorType,
		Value:      float64(20 + index),
		Unit:       getUnitForType(sensorType),
		Timestamp:  time.Now().Add(time.Duration(index) * time.Second),
		Tags: map[string]string{
			"location": "warehouse-A",
			"zone":     fmt.Sprintf("zone-%d", (index%3)+1),
			"source":   "consumer-group-example",
		},
	}
}

// getUnitForType returns the appropriate unit for a sensor type
func getUnitForType(sensorType string) string {
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

// routeToPartition calculates partition ID (same logic as consumer group)
func routeToPartition(event domain.SensorEvent, partitionCount int) int {
	hash := hashString(event.SensorID)
	partitionID := hash % partitionCount
	if partitionID < 0 {
		partitionID = -partitionID
	}
	return partitionID
}

// hashString computes a hash (same as consumer group implementation)
func hashString(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// ============================================================================
// FAILING MOCK INGESTER - For Demonstration
// ============================================================================
// This ingester randomly fails to demonstrate retry and DLQ behavior.
// In production, failures would come from actual infrastructure issues.

// FailingMockIngester is a mock ingester that randomly fails
type FailingMockIngester struct {
	logger      usecase.Logger
	failureRate float64 // Probability of failure (0.0 to 1.0)
	rand        *rand.Rand
}

// NewFailingMockIngester creates a new failing mock ingester
func NewFailingMockIngester(logger usecase.Logger, failureRate float64) *FailingMockIngester {
	return &FailingMockIngester{
		logger:      logger,
		failureRate: failureRate,
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Ingest simulates ingestion with random failures
func (m *FailingMockIngester) Ingest(ctx context.Context, event domain.SensorEvent) error {
	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Randomly fail based on failure rate
	if m.rand.Float64() < m.failureRate {
		// Simulate transient failure (could be network, database, etc.)
		return fmt.Errorf("simulated infrastructure failure for event %s", event.EventID)
	}

	// Success
	m.logger.Debug(fmt.Sprintf("Mock ingester: successfully processed event %s", event.EventID))
	return nil
}
