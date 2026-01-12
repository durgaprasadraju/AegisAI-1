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

	// Run the example demonstration using interfaces
	runExampleWithInterfaces(ctx)

	// In production, this would start the actual data-generator service
	// that continuously generates readings and sends them to Kafka
	<-ctx.Done()
}

// ============================================================================
// EXAMPLE USING INTERFACES - Clean Architecture Pattern
// ============================================================================
// This example demonstrates:
// 1. Dependency Inversion: We depend on interfaces, not concrete types
// 2. Testability: Easy to swap implementations for testing
// 3. Scalability: Can swap InMemoryPublisher with KafkaPublisher without code changes
// 4. Clean Architecture: Business logic doesn't depend on infrastructure details

// runExampleWithInterfaces demonstrates the power of interfaces in microservices.
// Notice how we use interfaces (SensorGenerator, Publisher, Storage) instead of
// concrete types. This allows us to:
// - Swap implementations without changing this code
// - Test with mock implementations
// - Gradually migrate from in-memory to Kafka/DynamoDB
func runExampleWithInterfaces(ctx context.Context) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		fmt.Println("Context cancelled, exiting example...")
		return
	default:
	}

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  AegisAI Data Generator - Interfaces & Functions (Day 3)      ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ============================================================================
	// STEP 1: Initialize Implementations (Dependency Injection)
	// ============================================================================
	// We create concrete implementations, but store them as interfaces.
	// This is dependency injection - we inject dependencies rather than creating them.
	fmt.Println("=== Step 1: Initialize Implementations (Dependency Injection) ===")

	// Create generators for different sensor types
	// These implement the SensorGenerator interface
	var generators []generator.SensorGenerator
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeTemperature, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeTemperature, 2))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeHumidity, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeMoisture, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypePressure, 1))

	// Create publisher (implements Publisher interface)
	// In production, this would be: publisher := NewKafkaPublisher(kafkaConfig)
	var publisher generator.Publisher = generator.NewInMemoryPublisher()

	// Create storage (implements Storage interface)
	// In production, this would be: storage := NewDynamoDBStorage(dynamoConfig)
	var storage generator.Storage = generator.NewInMemoryStorage()

	fmt.Println("Initialized:")
	fmt.Println("  - 5 SensorGenerators (random simulation)")
	fmt.Println("  - InMemoryPublisher (will be replaced with KafkaPublisher)")
	fmt.Println("  - InMemoryStorage (will be replaced with DynamoDBStorage)")
	fmt.Println()
	fmt.Println("Key Point: All variables are interfaces, not concrete types!")
	fmt.Println("  This means we can swap implementations without changing this code.")
	fmt.Println()

	// ============================================================================
	// STEP 2: Generate 5 Sensor Readings Using Interfaces
	// ============================================================================
	// We use the SensorGenerator interface, not the concrete RandomSensorGenerator.
	// This means we could swap in a RealSensorGenerator or HistoricalDataGenerator
	// without changing this code.
	fmt.Println("=== Step 2: Generate 5 Sensor Readings Using Interfaces ===")

	// Use pure functions to work with data
	readings := make([]generator.SensorData, 0, 5)

	// Generate readings using the interface
	for i, gen := range generators {
		reading := gen.Generate() // Call interface method, not concrete method
		reading.Timestamp = time.Now().Add(time.Duration(i) * time.Second)
		readings = append(readings, reading)
		fmt.Printf("  [%d] Generated via interface: %s\n", i+1, reading.String())
	}
	fmt.Println()

	// ============================================================================
	// STEP 3: Demonstrate Pure Functions
	// ============================================================================
	// Pure functions are stateless and easier to test.
	// They operate on data without side effects.
	fmt.Println("=== Step 3: Demonstrate Pure Functions ===")

	// Use pure function to add readings to a slice
	// This returns a new slice (immutability pattern)
	var readingsSlice []generator.SensorData
	for _, reading := range readings {
		readingsSlice = generator.AddReading(readingsSlice, reading)
	}

	fmt.Printf("Used AddReading() function: %d readings in slice\n", len(readingsSlice))

	// Use pure function to build latest readings map
	latestMap := make(map[string]generator.SensorData)
	for _, reading := range readings {
		latestMap = generator.UpdateLatestReadings(latestMap, reading)
	}

	fmt.Printf("Used UpdateLatestReadings() function: %d sensors in map\n", len(latestMap))
	fmt.Println()

	// ============================================================================
	// STEP 4: Publish Readings Using Interface
	// ============================================================================
	// We use the Publisher interface, not InMemoryPublisher.
	// In production, we'd swap this with KafkaPublisher and this code wouldn't change.
	fmt.Println("=== Step 4: Publish Readings Using Publisher Interface ===")
	fmt.Println("Publishing readings (using interface, not concrete type)...")

	for i, reading := range readings {
		if err := publisher.Publish(ctx, reading); err != nil {
			fmt.Printf("  Error publishing reading %d: %v\n", i+1, err)
			continue
		}
		fmt.Printf("  ✓ Published reading %d via Publisher interface\n", i+1)
	}
	fmt.Println()

	// Demonstrate batch publishing
	fmt.Println("Demonstrating batch publishing:")
	batch := readings[:3] // First 3 readings
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		fmt.Printf("Error publishing batch: %v\n", err)
	} else {
		fmt.Printf("  ✓ Published batch of %d readings\n", len(batch))
	}
	fmt.Println()

	// ============================================================================
	// STEP 5: Store Readings Using Interface
	// ============================================================================
	// We use the Storage interface, not InMemoryStorage.
	// In production, we'd swap this with DynamoDBStorage and this code wouldn't change.
	fmt.Println("=== Step 5: Store Readings Using Storage Interface ===")
	fmt.Println("Storing readings (using interface, not concrete type)...")

	for i, reading := range readings {
		if err := storage.Save(ctx, reading); err != nil {
			fmt.Printf("  Error storing reading %d: %v\n", i+1, err)
			continue
		}
		fmt.Printf("  ✓ Stored reading %d via Storage interface\n", i+1)
	}
	fmt.Println()

	// Demonstrate batch storage
	fmt.Println("Demonstrating batch storage:")
	if err := storage.SaveBatch(ctx, batch); err != nil {
		fmt.Printf("Error storing batch: %v\n", err)
	} else {
		fmt.Printf("  ✓ Stored batch of %d readings\n", len(batch))
	}
	fmt.Println()

	// ============================================================================
	// STEP 6: Retrieve Latest Readings Using Interface
	// ============================================================================
	fmt.Println("=== Step 6: Retrieve Latest Readings Using Storage Interface ===")

	sensorIDs := []string{"temp-001", "humidity-001", "pressure-001"}
	for _, sensorID := range sensorIDs {
		reading, exists, err := storage.GetLatest(ctx, sensorID)
		if err != nil {
			fmt.Printf("  Error retrieving %s: %v\n", sensorID, err)
			continue
		}
		if exists {
			fmt.Printf("  %s -> %.2f %s (retrieved via Storage interface)\n",
				sensorID, reading.Value, reading.Unit)
		} else {
			fmt.Printf("  %s -> not found\n", sensorID)
		}
	}
	fmt.Println()

	// ============================================================================
	// DEPENDENCY INVERSION EXPLANATION
	// ============================================================================
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              Dependency Inversion Principle                    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("WHAT WE DID:")
	fmt.Println("  • Used interfaces (SensorGenerator, Publisher, Storage)")
	fmt.Println("  • Not concrete types (RandomSensorGenerator, InMemoryPublisher)")
	fmt.Println()
	fmt.Println("WHY THIS MATTERS:")
	fmt.Println("  1. TESTING:")
	fmt.Println("     - Create MockPublisher that captures published data")
	fmt.Println("     - Test logic without needing Kafka running")
	fmt.Println("     - Fast, isolated unit tests")
	fmt.Println()
	fmt.Println("  2. GRADUAL MIGRATION:")
	fmt.Println("     - Start: InMemoryPublisher (logs to console)")
	fmt.Println("     - Next: KafkaPublisher (sends to Kafka)")
	fmt.Println("     - No code changes needed in this function!")
	fmt.Println()
	fmt.Println("  3. MULTIPLE ENVIRONMENTS:")
	fmt.Println("     - Dev: InMemoryPublisher")
	fmt.Println("     - Staging: KafkaPublisher with test topic")
	fmt.Println("     - Prod: KafkaPublisher with production topic")
	fmt.Println("     - Same code, different implementations")
	fmt.Println()
	fmt.Println("  4. SERVICE BOUNDARIES:")
	fmt.Println("     - data-generator: Uses Publisher interface")
	fmt.Println("     - ingestion-service: Implements Publisher (consumes from Kafka)")
	fmt.Println("     - ml-service: Uses Storage interface (queries DynamoDB)")
	fmt.Println("     - All services share the same interfaces, different implementations")
	fmt.Println()

	// ============================================================================
	// FUTURE INTEGRATION PATTERNS
	// ============================================================================
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              Future Integration Patterns                       ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("TO REPLACE InMemoryPublisher WITH KafkaPublisher:")
	fmt.Println("  // Just change the initialization:")
	fmt.Println("  publisher := NewKafkaPublisher(kafkaConfig)")
	fmt.Println("  // Rest of code stays the same!")
	fmt.Println()
	fmt.Println("TO REPLACE InMemoryStorage WITH DynamoDBStorage:")
	fmt.Println("  // Just change the initialization:")
	fmt.Println("  storage := NewDynamoDBStorage(dynamoConfig)")
	fmt.Println("  // Rest of code stays the same!")
	fmt.Println()
	fmt.Println("TO USE REAL SENSORS:")
	fmt.Println("  // Just change the generator:")
	fmt.Println("  generator := NewRealSensorGenerator(sensorConfig)")
	fmt.Println("  // Rest of code stays the same!")
	fmt.Println()
	fmt.Println("KEY INSIGHT:")
	fmt.Println("  Interfaces define WHAT we need (contract)")
	fmt.Println("  Implementations define HOW we do it (details)")
	fmt.Println("  Business logic depends on WHAT, not HOW")
	fmt.Println("  This is the Dependency Inversion Principle!")
	fmt.Println()
}
