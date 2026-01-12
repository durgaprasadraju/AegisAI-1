package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aegisai/data-generator/internal/generator"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down data-generator...")
		cancel()
	}()

	// Run the example demonstration
	runExample(ctx)

	// In production, this would start the actual data-generator service
	// that continuously generates readings and sends them to Kafka
	<-ctx.Done()
}

// runExample demonstrates the foundation of AegisAI data structures
// This shows how slices and maps work together in a distributed system context
func runExample(ctx context.Context) {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║     AegisAI Data Generator - Foundation Example (Day 2)        ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ============================================================================
	// STEP 1: Initialize the Data Manager
	// ============================================================================
	// The DataManager is the core component that manages sensor readings
	// in memory before sending them to Kafka in a distributed system.
	manager := generator.NewDataManager()

	fmt.Println("=== Step 1: Initialize Data Manager ===")
	fmt.Println("DataManager created with:")
	fmt.Printf("  - Readings slice: capacity for batch processing\n")
	fmt.Printf("  - Latest readings map: for O(1) lookups\n")
	fmt.Println()

	// ============================================================================
	// STEP 2: Generate 5 Sensor Readings
	// ============================================================================
	// In a real system, these would come from actual sensor hardware.
	// For now, we'll simulate readings for different sensor types.
	fmt.Println("=== Step 2: Generate 5 Sensor Readings ===")

	// Create readings for different sensor types
	readings := []generator.SensorData{
		// Temperature sensors
		generator.GenerateRandomReading(generator.TemperatureConfig, 1),
		generator.GenerateRandomReading(generator.TemperatureConfig, 2),
		// Humidity sensor
		generator.GenerateRandomReading(generator.HumidityConfig, 1),
		// Moisture sensor
		generator.GenerateRandomReading(generator.MoistureConfig, 1),
		// Pressure sensor
		generator.GenerateRandomReading(generator.PressureConfig, 1),
	}

	// Add small delays to create different timestamps
	for i := range readings {
		readings[i].Timestamp = time.Now().Add(time.Duration(i) * time.Second)
	}

	fmt.Println("Generated 5 sensor readings:")
	for i, reading := range readings {
		fmt.Printf("  [%d] %s\n", i+1, reading.String())
	}
	fmt.Println()

	// ============================================================================
	// STEP 3: Store Readings in Slice (via DataManager)
	// ============================================================================
	// The slice accumulates readings for batch processing.
	// In production, readings accumulate until we have a batch size,
	// then we send the entire batch to Kafka for better throughput.
	fmt.Println("=== Step 3: Store Readings in Slice ===")

	for i, reading := range readings {
		if err := manager.AddReading(reading); err != nil {
			fmt.Printf("Error adding reading %d: %v\n", i+1, err)
			continue
		}
		fmt.Printf("  ✓ Added reading %d to slice (total: %d)\n", i+1, manager.GetReadingsCount())
	}
	fmt.Println()

	// ============================================================================
	// STEP 4: Demonstrate Map of Latest Values
	// ============================================================================
	// The map automatically maintains the latest reading for each sensor ID.
	// This is useful for:
	// - Quick health checks (get latest value for any sensor)
	// - Alert evaluation (compare latest value against thresholds)
	// - API endpoints (expose current sensor state)
	fmt.Println("=== Step 4: Map of Latest Sensor Values ===")

	latestReadings := manager.GetAllLatestReadings()
	fmt.Printf("Latest readings map contains %d unique sensors:\n", len(latestReadings))
	for sensorID, reading := range latestReadings {
		fmt.Printf("  %s -> %.2f %s (at %s)\n",
			sensorID, reading.Value, reading.Unit, reading.Timestamp.Format("15:04:05"))
	}
	fmt.Println()

	// ============================================================================
	// STEP 5: Add More Readings (Simulating Continuous Data Stream)
	// ============================================================================
	// In a real system, sensors continuously generate new readings.
	// Let's simulate this by adding updated readings for existing sensors.
	fmt.Println("=== Step 5: Simulate Continuous Data Stream ===")
	fmt.Println("Adding updated readings for existing sensors...")

	// Generate new readings for the same sensors (simulating time passing)
	updatedReadings := []generator.SensorData{
		generator.GenerateRandomReading(generator.TemperatureConfig, 1), // Updated temp-001
		generator.GenerateRandomReading(generator.HumidityConfig, 1),    // Updated humidity-001
	}

	// Set timestamps to be newer
	for i := range updatedReadings {
		updatedReadings[i].Timestamp = time.Now().Add(time.Duration(10+i) * time.Second)
	}

	for _, reading := range updatedReadings {
		if err := manager.AddReading(reading); err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("  ✓ Added updated reading: %s\n", reading.String())
	}
	fmt.Println()

	// ============================================================================
	// STEP 6: Show How Map Automatically Updates Latest Values
	// ============================================================================
	fmt.Println("=== Step 6: Map Automatically Maintains Latest Values ===")
	fmt.Println("Latest readings after updates (map shows most recent per sensor):")

	latestReadings = manager.GetAllLatestReadings()
	for sensorID, reading := range latestReadings {
		fmt.Printf("  %s -> %.2f %s (at %s)\n",
			sensorID, reading.Value, reading.Unit, reading.Timestamp.Format("15:04:05"))
	}
	fmt.Println()

	// ============================================================================
	// STEP 7: Demonstrate Batch Processing (Kafka Integration Pattern)
	// ============================================================================
	// This shows how readings would be batched and sent to Kafka.
	fmt.Println("=== Step 7: Batch Processing (Kafka Integration Pattern) ===")
	fmt.Printf("Current batch size: %d readings\n", manager.GetReadingsCount())

	// Get the batch (this clears the internal slice but keeps the map)
	batch := manager.GetReadingsBatch()
	fmt.Printf("Retrieved batch of %d readings for Kafka\n", len(batch))
	fmt.Println("In production, this batch would be:")
	fmt.Println("  1. Serialized to JSON/Protobuf")
	fmt.Println("  2. Sent to Kafka topic 'sensor-data'")
	fmt.Println("  3. Partitioned by sensor ID for ordering")
	fmt.Println()

	// Show that slice is cleared but map still has latest values
	fmt.Printf("After batch retrieval:\n")
	fmt.Printf("  - Batch slice: %d readings (cleared, ready for next batch)\n", manager.GetReadingsCount())
	fmt.Printf("  - Latest map: %d sensors (still maintained for quick lookups)\n", manager.GetLatestReadingsCount())
	fmt.Println()

	// ============================================================================
	// STEP 8: Demonstrate O(1) Lookup from Map
	// ============================================================================
	fmt.Println("=== Step 8: O(1) Lookup Example ===")
	fmt.Println("Querying latest value for specific sensors (instant lookup):")

	sensorIDs := []string{"temp-001", "humidity-001", "pressure-001"}
	for _, sensorID := range sensorIDs {
		reading, exists := manager.GetLatestReading(sensorID)
		if exists {
			fmt.Printf("  %s -> %.2f %s\n", sensorID, reading.Value, reading.Unit)
		} else {
			fmt.Printf("  %s -> not found\n", sensorID)
		}
	}
	fmt.Println()

	// ============================================================================
	// INTEGRATION SUMMARY
	// ============================================================================
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    Integration Summary                         ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("DATA FLOW IN DISTRIBUTED SYSTEM:")
	fmt.Println("  1. data-generator:")
	fmt.Println("     - Generates sensor readings")
	fmt.Println("     - Accumulates in slice (batch processing)")
	fmt.Println("     - Maintains map (latest values)")
	fmt.Println("     - Sends batches to Kafka topic 'sensor-data'")
	fmt.Println()
	fmt.Println("  2. ingestion-service:")
	fmt.Println("     - Consumes from Kafka")
	fmt.Println("     - Stores time-series data in DynamoDB")
	fmt.Println("     - Updates latest values table")
	fmt.Println()
	fmt.Println("  3. ml-service:")
	fmt.Println("     - Queries latest values from DynamoDB")
	fmt.Println("     - Performs ML inference")
	fmt.Println("     - Returns predictions via gRPC")
	fmt.Println()
	fmt.Println("  4. alert-service:")
	fmt.Println("     - Monitors latest values from map/DB")
	fmt.Println("     - Evaluates thresholds")
	fmt.Println("     - Triggers alerts when thresholds exceeded")
	fmt.Println()
	fmt.Println("KEY DATA STRUCTURES:")
	fmt.Println("  • Slice: Batch processing for Kafka (better throughput)")
	fmt.Println("  • Map: O(1) lookup for latest values (health checks, alerts)")
	fmt.Println("  • Struct: Type-safe data model (flows through all services)")
	fmt.Println()
}
