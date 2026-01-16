package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
	"github.com/aegisai/data-generator/internal/generator"
	"github.com/aegisai/data-generator/internal/usecase"
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

	// Run the example demonstration using interfaces (Day 3)
	runExampleWithInterfaces(ctx)

	// Optional: Run consumer group example with DLQ (Day 6)
	// Enable by setting environment variable: export RUN_CONSUMER_GROUP_DEMO=true
	if os.Getenv("RUN_CONSUMER_GROUP_DEMO") == "true" {
		runConsumerGroupWithDLQExample(ctx)
	}

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
	fmt.Println("║  AegisAI Data Generator - Kafka & DynamoDB (Day 3)           ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("This demonstration shows:")
	fmt.Println("  • Kafka integration (local Docker Kafka)")
	fmt.Println("  • DynamoDB integration (LocalStack)")
	fmt.Println("  • Same code works in production with only config changes")
	fmt.Println()

	// ============================================================================
	// STEP 1: Initialize Implementations (Dependency Injection)
	// ============================================================================
	// We create concrete implementations, but store them as interfaces.
	// This is dependency injection - we inject dependencies rather than creating them.
	fmt.Println("=== Step 1: Initialize Implementations (Dependency Injection) ===")

	// Load configuration from environment variables
	// These configs work with local Kafka (Docker) and LocalStack (DynamoDB)
	// In production, only the environment variables change, not the code
	kafkaConfig := generator.LoadKafkaConfig()
	dynamoConfig := generator.LoadDynamoConfig()

	fmt.Printf("Kafka Config: broker=%s, topic=%s\n", kafkaConfig.Broker, kafkaConfig.Topic)
	fmt.Printf("DynamoDB Config: endpoint=%s, region=%s, table=%s\n",
		dynamoConfig.Endpoint, dynamoConfig.Region, dynamoConfig.Table)
	fmt.Println()

	// Create generators for different sensor types
	// These implement the SensorGenerator interface
	var generators []generator.SensorGenerator
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeTemperature, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeTemperature, 2))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeHumidity, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypeMoisture, 1))
	generators = append(generators, generator.NewRandomSensorGenerator(generator.SensorTypePressure, 1))

	// Create publisher (implements Publisher interface)
	// This uses KafkaLocalPublisher which connects to local Kafka (Docker)
	// In production, same code works with AWS MSK - just change KAFKA_BROKER env var
	publisher, err := generator.NewKafkaLocalPublisher(kafkaConfig)
	if err != nil {
		fmt.Printf("Error initializing Kafka publisher: %v\n", err)
		fmt.Println("Make sure Kafka is running (docker-compose up kafka)")
		return
	}
	defer publisher.Close() // Ensure cleanup on exit

	// Create storage (implements Storage interface)
	// This uses DynamoLocalStorage which connects to LocalStack
	// In production, same code works with real DynamoDB - just change DYNAMO_ENDPOINT env var
	storage, err := generator.NewDynamoLocalStorage(dynamoConfig)
	if err != nil {
		fmt.Printf("Error initializing DynamoDB storage: %v\n", err)
		fmt.Println("Make sure LocalStack is running (docker-compose up localstack)")
		return
	}

	fmt.Println("Initialized:")
	fmt.Println("  - 5 SensorGenerators (random simulation)")
	fmt.Println("  - KafkaLocalPublisher (connects to local Kafka at " + kafkaConfig.Broker + ")")
	fmt.Println("  - DynamoLocalStorage (connects to LocalStack at " + dynamoConfig.Endpoint + ")")
	fmt.Println()
	fmt.Println("Key Point: All variables are interfaces, not concrete types!")
	fmt.Println("  This means we can swap implementations without changing this code.")
	fmt.Println("  Local Kafka and LocalStack use the same protocols as production!")
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
	fmt.Println("     - Day 2: InMemoryPublisher (logs to console)")
	fmt.Println("     - Day 3: KafkaLocalPublisher (sends to local Kafka)")
	fmt.Println("     - Production: Same code, change KAFKA_BROKER env var to MSK")
	fmt.Println("     - No code changes needed!")
	fmt.Println()
	fmt.Println("  3. MULTIPLE ENVIRONMENTS:")
	fmt.Println("     - Local: KafkaLocalPublisher (localhost:9092)")
	fmt.Println("     - Staging: Same code, KAFKA_BROKER=staging-kafka:9092")
	fmt.Println("     - Prod: Same code, KAFKA_BROKER=msk-broker:9092")
	fmt.Println("     - Same code, different configurations")
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
	fmt.Println("CURRENT IMPLEMENTATION (Day 3):")
	fmt.Println("  • KafkaLocalPublisher: Connects to local Kafka (Docker)")
	fmt.Println("  • DynamoLocalStorage: Connects to LocalStack")
	fmt.Println("  • Both use same protocols as production!")
	fmt.Println()
	fmt.Println("TO MIGRATE TO PRODUCTION:")
	fmt.Println("  // No code changes needed!")
	fmt.Println("  // Just set environment variables:")
	fmt.Println("  export KAFKA_BROKER=msk-broker-1:9092,msk-broker-2:9092")
	fmt.Println("  export DYNAMO_ENDPOINT=  # Empty = use real AWS")
	fmt.Println("  export AWS_REGION=us-east-1")
	fmt.Println("  // Same code, different config!")
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

// ============================================================================
// CONSUMER GROUP WITH DLQ EXAMPLE - Day 6 Advanced Features
// ============================================================================
// This demonstrates the ConsumerGroup with Kafka DLQ publisher integration.
// This shows how failed events are persisted to Kafka DLQ topic for later analysis.

// runConsumerGroupWithDLQExample demonstrates ConsumerGroup with DLQ Kafka publishing.
// This integrates the consumer group use case with the DLQ Kafka publisher.
func runConsumerGroupWithDLQExample(ctx context.Context) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Consumer Group with DLQ Kafka Publisher (Day 6)            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// ============================================================================
	// STEP 1: Initialize DLQ Kafka Publisher
	// ============================================================================
	fmt.Println("=== Step 1: Initialize DLQ Kafka Publisher ===")

	kafkaConfig := generator.LoadKafkaConfig()
	fmt.Printf("Kafka Config: broker=%s, topic=%s, dlq_topic=%s\n",
		kafkaConfig.Broker, kafkaConfig.Topic, kafkaConfig.DLQTopic)

	// Create DLQ ingester (Kafka publisher for DLQ topic)
	dlqIngester, err := generator.NewKafkaDLQIngester(kafkaConfig)
	if err != nil {
		fmt.Printf("Error initializing DLQ Kafka publisher: %v\n", err)
		fmt.Println("DLQ will use logging-only mode (no Kafka publishing)")
		dlqIngester = nil
	} else {
		defer dlqIngester.Close()
		fmt.Printf("✓ DLQ Kafka publisher initialized (topic: %s)\n", kafkaConfig.DLQTopic)
	}
	fmt.Println()

	// ============================================================================
	// STEP 2: Setup Consumer Group with DLQ
	// ============================================================================
	fmt.Println("=== Step 2: Setup Consumer Group with DLQ ===")

	logger := usecase.NewSimpleLogger()

	// Create a simple ingester that publishes to main Kafka topic
	// In production, this would be a KafkaIngester that publishes to the main topic
	mainIngester := &SimpleKafkaIngester{
		kafkaConfig: kafkaConfig,
		logger:      logger,
	}
	defer mainIngester.Close() // Ensure cleanup on exit

	// Create single event use case
	singleUseCase := usecase.NewIngestSensorUseCase(mainIngester, logger)

	// Load consumer group configuration
	config := usecase.LoadConsumerGroupConfig()

	// Create consumer group with DLQ ingester
	consumerGroup := usecase.NewConsumerGroupUseCase(config, singleUseCase, logger, dlqIngester)
	fmt.Println("✓ Consumer group created with DLQ support")
	fmt.Printf("  - Partitions: %d\n", config.PartitionCount)
	fmt.Printf("  - Workers per partition: %d\n", config.WorkersPerPartition)
	fmt.Printf("  - Max retries: %d\n", config.MaxRetries)
	if dlqIngester != nil {
		fmt.Printf("  - DLQ: Publishing to Kafka topic '%s'\n", kafkaConfig.DLQTopic)
	} else {
		fmt.Println("  - DLQ: Logging only (no Kafka publisher)")
	}
	fmt.Println()

	// ============================================================================
	// STEP 3: Start Consumer Group
	// ============================================================================
	fmt.Println("=== Step 3: Start Consumer Group ===")

	if err := consumerGroup.Start(ctx); err != nil {
		fmt.Printf("Error starting consumer group: %v\n", err)
		return
	}
	fmt.Println("✓ Consumer group started")
	fmt.Println()

	// ============================================================================
	// STEP 4: Generate and Submit Events
	// ============================================================================
	fmt.Println("=== Step 4: Generate and Submit Events ===")

	// Generate some events using the new GenerateSensorEvent function
	for i := 0; i < 10; i++ {
		event := generator.GenerateSensorEvent(
			generator.SensorTypeTemperature,
			1, // sensor number
			i, // event index
		)

		if err := consumerGroup.Ingest(event); err != nil {
			fmt.Printf("  ✗ Error submitting event %d: %v\n", i+1, err)
		} else {
			fmt.Printf("  ✓ Submitted event %d: %s (sensor: %s)\n", i+1, event.EventID, event.SensorID)
		}
	}
	fmt.Println()

	// ============================================================================
	// STEP 5: Wait for Processing
	// ============================================================================
	fmt.Println("=== Step 5: Wait for Processing (including retries and DLQ) ===")
	fmt.Println("Waiting for events to be processed...")
	time.Sleep(3 * time.Second)
	fmt.Println()

	// ============================================================================
	// STEP 6: Graceful Shutdown
	// ============================================================================
	fmt.Println("=== Step 6: Graceful Shutdown ===")
	consumerGroup.Shutdown()
	fmt.Println("✓ Consumer group shut down complete")
	fmt.Println()

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              Consumer Group + DLQ Demo Complete               ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Key Points:")
	fmt.Println("  • Events processed through ConsumerGroup with partitions")
	fmt.Println("  • Failed events retried with exponential backoff")
	fmt.Println("  • Events exceeding max retries published to Kafka DLQ topic")
	fmt.Println("  • DLQ topic: " + kafkaConfig.DLQTopic)
	fmt.Println()
}

// SimpleKafkaIngester is a simple ingester that publishes to the main Kafka topic.
// This is used for the ConsumerGroup demonstration in main.go.
//
// Thread-Safety:
// - Uses mutex-protected lazy initialization for the Kafka publisher
// - Safe for concurrent use by multiple worker goroutines
// - KafkaLocalPublisher itself is thread-safe (kafka-go writer is concurrent-safe)
type SimpleKafkaIngester struct {
	kafkaConfig *generator.KafkaConfig
	logger      usecase.Logger
	publisher   *generator.KafkaLocalPublisher
	mu          sync.Mutex // Protects publisher initialization
}

// Ingest publishes the event to the main Kafka topic.
// This implements the SensorIngester interface.
//
// Thread-Safety:
// - Uses double-check locking pattern for thread-safe lazy initialization
// - Safe to call from multiple goroutines concurrently
func (s *SimpleKafkaIngester) Ingest(ctx context.Context, event domain.SensorEvent) error {
	// Thread-safe lazy initialization using double-check locking pattern
	if s.publisher == nil {
		s.mu.Lock()
		// Double-check after acquiring lock (another goroutine might have initialized)
		if s.publisher == nil {
			var err error
			s.publisher, err = generator.NewKafkaLocalPublisher(s.kafkaConfig)
			if err != nil {
				s.mu.Unlock()
				return fmt.Errorf("failed to create Kafka publisher: %w", err)
			}
		}
		s.mu.Unlock()
	}

	// Convert SensorEvent to SensorData for publishing
	// Note: This is a simplified conversion - in production, you might want
	// a more sophisticated adapter that handles the conversion properly
	sensorData := generator.SensorData{
		ID:        event.SensorID,
		Type:      event.SensorType,
		Value:     event.Value,
		Unit:      event.Unit,
		Timestamp: event.Timestamp,
	}

	// Publish to Kafka main topic
	// Note: KafkaLocalPublisher.Publish() is thread-safe - can be called concurrently
	return s.publisher.Publish(ctx, sensorData)
}

// Close closes the Kafka publisher and releases resources.
// This should be called when the ingester is no longer needed.
func (s *SimpleKafkaIngester) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publisher != nil {
		return s.publisher.Close()
	}
	return nil
}
