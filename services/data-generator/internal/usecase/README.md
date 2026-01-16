# Use Case Layer

Business logic and orchestration layer implementing concurrent processing patterns, following Clean Architecture principles.

## Architecture

This package contains:
- **Use Cases**: Business logic for ingesting sensor events
- **Ports**: Interfaces that use cases depend on (dependency inversion)
- **Concurrency Patterns**: Worker pools, channels, consumer groups
- **Configuration**: Environment-based settings

**Dependencies:**
- ✅ `domain` package (business entities)
- ✅ Port interfaces (abstractions)
- ❌ NO infrastructure (Kafka, DynamoDB, AWS SDK)

## Files

### `ingest_port.go` - Port Interface
Defines the `SensorIngester` interface (port in Clean Architecture).

**Purpose:**
- Use cases depend on this interface, not concrete implementations
- Allows swapping implementations (Kafka, SQS, HTTP, etc.)
- Enables testing with mock implementations

**Interface:**
```go
type SensorIngester interface {
    Ingest(ctx context.Context, event domain.SensorEvent) error
}
```

**Usage:**
- Implemented by adapters in infrastructure layer
- Used by use cases via dependency injection

### `logger.go` - Logger Interface
Simple logging abstraction for use cases.

**Purpose:**
- Keeps use cases independent of logging libraries
- Easy to mock for testing
- Can swap implementations without changing use case code

**Interface:**
```go
type Logger interface {
    Info(msg string)
    Error(msg string, err error)
    Debug(msg string)
}
```

**Implementation:**
- `SimpleLogger`: Basic stdout logger (for development)

### `config.go` - Configuration
Loads configuration from environment variables.

**Configurations:**
1. **PipelineConfig**: For simple worker pool pipeline
   - `WorkerCount`: Number of workers
   - `ChannelBufferSize`: Channel buffer size

2. **ConsumerGroupConfig**: For Kafka-like consumer group
   - `PartitionCount`: Number of partitions
   - `WorkersPerPartition`: Workers per partition
   - `MaxRetries`: Maximum retry attempts
   - `DLQBufferSize`: Dead Letter Queue buffer
   - `BackpressureMode`: "blocking" or "dropping"

**Functions:**
- `LoadPipelineConfig()`: Loads pipeline configuration
- `LoadConsumerGroupConfig()`: Loads consumer group configuration

### `ingest_sensor.go` - Single Event Use Case
Processes a single sensor event (unit of work).

**Purpose:**
- Validates sensor event
- Calls `SensorIngester` port to persist/publish
- Handles errors gracefully

**Struct:**
```go
type IngestSensorUseCase struct {
    ingester SensorIngester  // Port interface
    logger   Logger          // Logger interface
}
```

**Method:**
- `Execute(ctx, event)`: Processes single event

**Usage:**
- Called by pipeline workers
- Called by consumer group workers
- Can be used standalone

### `ingest_pipeline.go` - Concurrent Pipeline (Day 5)
Implements worker pool pattern for concurrent ingestion.

**Purpose:**
- Processes events concurrently using worker pool
- Uses buffered channels for event queuing
- Supports graceful shutdown

**Struct:**
```go
type IngestPipelineUseCase struct {
    config        *PipelineConfig
    singleUseCase *IngestSensorUseCase
    logger        Logger
    inputChan     chan domain.SensorEvent
    // ... concurrency primitives
}
```

**Key Features:**
- Worker pool: Configurable number of workers
- Buffered channel: Non-blocking producer
- Context cancellation: Graceful shutdown
- Error handling: Logs errors, doesn't crash

**Methods:**
- `Start(ctx)`: Starts worker pool
- `Ingestion(event)`: Submits event (non-blocking)
- `Shutdown()`: Gracefully shuts down

**Why Worker Pool?**
- Controls concurrency (prevents resource exhaustion)
- Predictable resource usage
- Mirrors Kafka consumer group behavior

### `consumer_group.go` - Kafka Consumer Group Simulation (Day 6)
Simulates Kafka consumer group with partitions, retries, and DLQ.

**Purpose:**
- Partition-based parallelism
- Ordering guarantees per partition
- Retry mechanism with exponential backoff
- Dead Letter Queue for failed events
- Backpressure handling

**Struct:**
```go
type ConsumerGroupUseCase struct {
    config        *ConsumerGroupConfig
    singleUseCase *IngestSensorUseCase
    logger        Logger
    partitions    []chan domain.SensorEvent  // One per partition
    dlq           chan domain.SensorEvent    // Dead Letter Queue
    // ... concurrency primitives
}
```

**Key Features:**

1. **Partition Routing:**
   - Hash-based: `hash(SensorID) % partitionCount`
   - Same SensorID → same partition (ordering guarantee)
   - Even distribution across partitions

2. **Worker Pools Per Partition:**
   - Multiple workers per partition for throughput
   - Events within partition processed in order
   - Events across partitions processed in parallel

3. **Retry Mechanism:**
   - Exponential backoff: `baseDelay * (2 ^ retryCount)`
   - Configurable max retries
   - Prevents overwhelming downstream systems

4. **Dead Letter Queue:**
   - Failed events after max retries
   - Separate consumer goroutine
   - Prevents infinite retry loops

5. **Backpressure Handling:**
   - **Blocking mode**: Waits for space (guarantees delivery)
   - **Dropping mode**: Drops events when full (higher throughput)

**Methods:**
- `Start(ctx)`: Starts consumer group (partitions + DLQ)
- `Ingest(event)`: Routes event to partition
- `Shutdown()`: Gracefully shuts down

**How it Maps to Kafka:**
- Partitions = Kafka topic partitions
- Workers = Consumer group members
- Ordering = Per partition (same key → same partition)
- Retry = Exponential backoff (standard pattern)
- DLQ = Dead Letter Queue topic

## Usage Examples

### Simple Pipeline
```go
config := usecase.LoadPipelineConfig()
singleUseCase := usecase.NewIngestSensorUseCase(ingester, logger)
pipeline := usecase.NewIngestPipelineUseCase(config, singleUseCase, logger)

pipeline.Start(ctx)
pipeline.Ingestion(event)
pipeline.Shutdown()
```

### Consumer Group
```go
config := usecase.LoadConsumerGroupConfig()
singleUseCase := usecase.NewIngestSensorUseCase(ingester, logger)

// Optional: Create DLQ ingester to publish failed events to Kafka DLQ topic
// If nil, failed events will only be logged
var dlqIngester usecase.SensorIngester // Can be nil for logging-only mode

consumerGroup := usecase.NewConsumerGroupUseCase(config, singleUseCase, logger, dlqIngester)

consumerGroup.Start(ctx)
consumerGroup.Ingest(event)
consumerGroup.Shutdown()
```

## Concurrency Patterns

### Channels
- Thread-safe communication
- Built-in backpressure
- Clean shutdown semantics

### Worker Pools
- Controls concurrency
- Predictable resource usage
- Horizontal scaling

### Context
- Graceful cancellation
- Timeout support
- No goroutine leaks

## Error Handling

- Errors are logged but don't stop processing
- Failed events can be retried
- Events that exceed max retries go to DLQ
- Pipeline continues even if individual events fail

## Testing

Use cases are easily testable with mock implementations:

```go
mockIngester := &MockIngester{}
logger := usecase.NewSimpleLogger()
useCase := usecase.NewIngestSensorUseCase(mockIngester, logger)
```
