package generator

import "context"

// ============================================================================
// INTERFACES - Service Boundaries and Contracts
// ============================================================================
// Interfaces define contracts that different implementations must satisfy.
// This is the foundation of dependency inversion and clean architecture.
//
// Why interfaces are critical in microservices:
// 1. Decoupling: Services depend on abstractions, not concrete implementations
// 2. Testability: Easy to create mock implementations for testing
// 3. Flexibility: Swap implementations without changing calling code
// 4. Scalability: Different services can implement interfaces differently
// 5. Integration: Allows gradual migration (e.g., in-memory -> Kafka -> DynamoDB)

// SensorGenerator defines the contract for generating sensor data.
// Different implementations can generate data from:
// - Random simulation (for testing)
// - Real hardware sensors (production)
// - Historical data replay (for testing)
// - External APIs (third-party sensor networks)
//
// This interface allows the data-generator service to work with any data source
// without knowing the implementation details.
type SensorGenerator interface {
	// Generate creates a new sensor reading.
	// Returns the generated SensorData.
	Generate() SensorData
}

// Publisher defines the contract for publishing sensor data to a message queue.
// Different implementations can publish to:
// - In-memory (for testing, logs)
// - Kafka (production - AWS MSK)
// - RabbitMQ (alternative message broker)
// - SQS (AWS Simple Queue Service)
//
// This interface allows swapping message brokers without changing the core logic.
// The data-generator service doesn't need to know if it's sending to Kafka or SQS.
type Publisher interface {
	// Publish sends sensor data to the message queue.
	// Returns an error if publishing fails.
	// In production, this would be asynchronous, but for simplicity we use synchronous.
	Publish(ctx context.Context, data SensorData) error

	// PublishBatch sends multiple sensor readings in a batch.
	// Batching improves throughput and reduces network overhead.
	// Returns an error if publishing fails.
	PublishBatch(ctx context.Context, data []SensorData) error
}

// Storage defines the contract for persisting sensor data.
// Different implementations can store data in:
// - In-memory (for testing, development)
// - DynamoDB (production - time-series data)
// - PostgreSQL (alternative database)
// - S3 (for archival, large datasets)
//
// This interface allows the ingestion-service to work with any storage backend.
// When we migrate from in-memory to DynamoDB, we only change the implementation,
// not the code that uses the Storage interface.
type Storage interface {
	// Save stores a single sensor reading.
	// Returns an error if storage fails.
	Save(ctx context.Context, data SensorData) error

	// SaveBatch stores multiple sensor readings in a single operation.
	// Batch operations are more efficient than individual saves.
	// Returns an error if storage fails.
	SaveBatch(ctx context.Context, data []SensorData) error

	// GetLatest retrieves the most recent reading for a sensor ID.
	// Returns the reading and a boolean indicating if it was found.
	GetLatest(ctx context.Context, sensorID string) (SensorData, bool, error)
}

// ============================================================================
// INTERFACE COMPOSITION
// ============================================================================
// Go allows composing interfaces, which is useful for creating more specific contracts.

// SensorService combines generation, publishing, and storage.
// This could be used by a service that needs all three capabilities.
type SensorService interface {
	SensorGenerator
	Publisher
	Storage
}

// ============================================================================
// WHY INTERFACES IN DISTRIBUTED SYSTEMS?
// ============================================================================
//
// 1. SERVICE BOUNDARIES:
//    - Each microservice can implement interfaces differently
//    - data-generator uses RandomSensorGenerator (simulation)
//    - ml-service might use HistoricalDataGenerator (for training)
//    - Both satisfy the same interface, so they're interchangeable
//
// 2. GRADUAL MIGRATION:
//    - Start with InMemoryPublisher (logs to console)
//    - Migrate to KafkaPublisher when infrastructure is ready
//    - No code changes needed in the service logic
//
// 3. TESTING:
//    - Create MockPublisher that captures published data
//    - Test service logic without needing Kafka running
//    - Fast, isolated unit tests
//
// 4. MULTIPLE IMPLEMENTATIONS:
//    - Dev environment: InMemoryPublisher
//    - Staging: KafkaPublisher with test topic
//    - Production: KafkaPublisher with production topic
//    - Same code, different implementations
//
// 5. DEPENDENCY INVERSION:
//    - High-level modules (service logic) don't depend on low-level modules (Kafka)
//    - Both depend on abstractions (interfaces)
//    - Makes the system more flexible and maintainable
