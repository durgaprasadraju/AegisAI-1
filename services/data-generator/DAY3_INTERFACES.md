# Day 3: Functions and Interfaces - Clean Architecture

## Overview

Day 3 introduces **functions** and **interfaces** to create clean service boundaries in the AegisAI distributed system. This follows the Dependency Inversion Principle and Clean Architecture patterns.

## What We Built

### 1. Pure Functions (`functions.go`)

Pure functions are stateless and operate on data without side effects:

- `GenerateSensorData(sensorType string, sensorNumber int) SensorData`
- `AddReading(readings []SensorData, data SensorData) []SensorData`
- `UpdateLatestReadings(latest map[string]SensorData, data SensorData) map[string]SensorData`

**Why Pure Functions?**
- Easier to test (no hidden state)
- Predictable behavior
- Can be used in functional programming patterns
- Thread-safe by default

### 2. Interfaces (`interfaces.go`)

Interfaces define contracts that implementations must satisfy:

- `SensorGenerator`: Contract for generating sensor data
- `Publisher`: Contract for publishing to message queues
- `Storage`: Contract for persisting data

**Why Interfaces?**
- **Dependency Inversion**: Depend on abstractions, not concrete types
- **Testability**: Easy to create mocks
- **Flexibility**: Swap implementations without changing calling code
- **Scalability**: Different services can implement differently

### 3. Concrete Implementations (`implementations.go`)

In-memory implementations for development/testing:

- `RandomSensorGenerator`: Generates random sensor readings
- `InMemoryPublisher`: Logs data (replaces with KafkaPublisher later)
- `InMemoryStorage`: Stores in memory (replaces with DynamoDBStorage later)

## Key Concepts Explained

### Dependency Inversion Principle

**Traditional (Bad):**
```go
// High-level code depends on low-level code
func processData() {
    kafka := NewKafkaPublisher()  // Direct dependency on Kafka
    kafka.Publish(data)
}
```

**With Interfaces (Good):**
```go
// High-level code depends on abstraction (interface)
func processData(publisher Publisher) {  // Depends on interface
    publisher.Publish(data)  // Doesn't know if it's Kafka or in-memory
}
```

### How This Helps Testing

```go
// Create a mock for testing
type MockPublisher struct {
    published []SensorData
}

func (m *MockPublisher) Publish(ctx context.Context, data SensorData) error {
    m.published = append(m.published, data)
    return nil
}

// Test without needing Kafka
func TestProcessData(t *testing.T) {
    mock := &MockPublisher{}
    processData(mock)
    assert.Equal(t, 1, len(mock.published))
}
```

### How This Helps Scaling

**Development:**
```go
publisher := NewInMemoryPublisher()  // Just logs
```

**Production:**
```go
publisher := NewKafkaPublisher(kafkaConfig)  // Sends to Kafka
```

**Same code, different implementation!**

## Migration Path

### Current (Day 3)
- `InMemoryPublisher` → Logs to console
- `InMemoryStorage` → Stores in memory

### Future (Production)
- `KafkaPublisher` → Sends to AWS MSK
- `DynamoDBStorage` → Stores in DynamoDB

### How to Migrate

1. **Implement new concrete type:**
   ```go
   type KafkaPublisher struct {
       producer *kafka.Producer
   }
   
   func (p *KafkaPublisher) Publish(ctx context.Context, data SensorData) error {
       // Send to Kafka
   }
   ```

2. **Swap in main.go:**
   ```go
   // Change this:
   publisher := NewInMemoryPublisher()
   
   // To this:
   publisher := NewKafkaPublisher(kafkaConfig)
   ```

3. **That's it!** No other code changes needed.

## Service Integration

### data-generator Service
- Uses `SensorGenerator` interface to generate data
- Uses `Publisher` interface to send data
- Doesn't know if it's Kafka or in-memory

### ingestion-service (Future)
- Implements `Publisher` interface (consumes from Kafka)
- Uses `Storage` interface to save data
- Doesn't know if it's DynamoDB or in-memory

### ml-service (Future)
- Uses `Storage` interface to query data
- Doesn't know if it's DynamoDB or in-memory

### All Services Share Interfaces
- Same contracts, different implementations
- Easy to test each service independently
- Easy to swap implementations per environment

## Running the Example

```bash
cd services/data-generator
go run cmd/main.go
```

This demonstrates:
1. Creating generators, publisher, and storage as interfaces
2. Generating 5 sensor readings
3. Publishing using the Publisher interface
4. Storing using the Storage interface
5. Retrieving latest readings

## Key Takeaways

1. **Interfaces define WHAT** (contracts)
2. **Implementations define HOW** (details)
3. **Business logic depends on WHAT, not HOW**
4. **This enables:**
   - Easy testing (mocks)
   - Gradual migration (swap implementations)
   - Multiple environments (dev/staging/prod)
   - Service boundaries (shared contracts)

## Next Steps

1. Implement `KafkaPublisher` for production
2. Implement `DynamoDBStorage` for persistence
3. Add unit tests using mock implementations
4. Add integration tests with real implementations
5. Implement `RealSensorGenerator` for hardware sensors
