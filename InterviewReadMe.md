# AegisAI Data Generator - Architecture Documentation

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Principles](#architecture-principles)
3. [Layer-by-Layer Architecture](#layer-by-layer-architecture)
4. [Consumer Group Implementation](#consumer-group-implementation)
5. [Dead Letter Queue (DLQ) Architecture](#dead-letter-queue-dlq-architecture)
6. [Data Flow](#data-flow)
7. [Design Patterns](#design-patterns)
8. [Key Components](#key-components)
9. [Error Handling & Resilience](#error-handling--resilience)
10. [Scalability & Performance](#scalability--performance)

---

## System Overview

AegisAI Data Generator is a production-ready microservice built with **Clean Architecture** principles. It demonstrates distributed systems concepts including:

- **Partition-based parallelism** (Kafka Consumer Group simulation)
- **Concurrent event processing** (Worker pools)
- **Retry mechanisms** (Exponential backoff)
- **Dead Letter Queue** (DLQ) for failed events
- **Backpressure handling** (Blocking/Dropping modes)
- **Interface-based design** (Dependency Inversion Principle)

### Core Capabilities

1. **Event Generation**: Generates sensor events with configurable types and values
2. **Event Ingestion**: Processes events through concurrent pipelines
3. **Partition Routing**: Routes events to partitions based on SensorID hash
4. **Retry Logic**: Handles transient failures with exponential backoff
5. **DLQ Publishing**: Publishes permanently failed events to Kafka DLQ topic
6. **Graceful Shutdown**: Handles context cancellation and resource cleanup

---

## Event Generation

Event generation is a critical component that creates sensor reading events. The system supports multiple generation approaches:

### Generation Locations

#### 1. Domain Model: `SensorEvent` Generation (Primary)

**Location: `examples/consumer_group_example.go`**
- **Function**: `generateSensorEvent(index int, sensorID string) domain.SensorEvent`
- **Line**: ~226
- **Purpose**: Generates `domain.SensorEvent` for Consumer Group example
- **Features**:
  - Sequential EventIDs (`event-001`, `event-002`, etc.)
  - Rotating sensor types (temperature, humidity, pressure, moisture)
  - Configurable sensor IDs
  - Metadata tags (location, zone, source)

**Location: `examples/pipeline_example.go`**
- **Function**: `generatePipelineSensorEvent(index int) domain.SensorEvent`
- **Line**: ~146
- **Purpose**: Generates `domain.SensorEvent` for Pipeline example
- **Features**: Similar to consumer group, but with pipeline-specific tags

**Example Code:**
```go
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
```

#### 2. Generator Layer: `SensorEvent` Generation Utilities

**Location: `internal/generator/functions.go`**
- **Function**: `GenerateSensorEvent(sensorType string, sensorNumber int, eventIndex int) domain.SensorEvent`
- **Line**: ~136
- **Purpose**: Pure function to generate `domain.SensorEvent` based on configuration
- **Design**: Stateless, testable, functional programming approach
- **Returns**: Complete `domain.SensorEvent` with EventID, Tags, and all metadata

**Location: `internal/generator/functions.go`**
- **Function**: `GenerateSensorEvents(sensorType string, sensorNumber int, count int) []domain.SensorEvent`
- **Line**: ~181
- **Purpose**: Batch generation of multiple `SensorEvent` instances
- **Use Case**: Useful for examples, tests, and load testing

**Example Usage:**
```go
// Generate a single event
event := generator.GenerateSensorEvent(
    generator.SensorTypeTemperature, 
    1,  // sensor number
    0,  // event index
)

// Generate multiple events
events := generator.GenerateSensorEvents(
    generator.SensorTypeTemperature,
    1,  // sensor number
    10, // count
)
```

**Note**: The generator layer provides `GenerateSensorEvent()` which returns `domain.SensorEvent` - the preferred domain model used throughout the use case layer. The older `SensorData` type is still available for backward compatibility with infrastructure adapters (Publisher, Storage interfaces).

### Generation Strategy

#### Configuration-Based Generation

Sensor values are generated based on pre-configured ranges:

**Sensor Configurations** (`internal/generator/sensor.go`):
- **Temperature**: -10.0°C to 50.0°C
- **Humidity**: 0% to 100%
- **Moisture**: 0% to 100%
- **Pressure**: 980 Pa to 1050 Pa

**Random Value Generation:**
```go
func (config SensorConfig) GenerateRandomValue() float64 {
    rangeSize := config.MaxValue - config.MinValue
    return config.MinValue + rand.Float64()*rangeSize
}
```

#### Event Metadata

Generated events include:
- **EventID**: Unique identifier (e.g., `event-001`)
- **SensorID**: Sensor identifier (e.g., `sensor-001`)
- **SensorType**: Type of sensor (temperature, humidity, etc.)
- **Value**: Random value within configured range
- **Unit**: Measurement unit (celsius, percent, pascal)
- **Timestamp**: When reading was taken
- **Tags**: Key-value metadata (location, zone, source, etc.)

### Generation Flow

```
Configuration (SensorConfig)
    ↓
Random Value Generation
    ↓
EventID Generation
    ↓
Metadata Addition (Tags)
    ↓
domain.SensorEvent Creation
    ↓
Event Submission to Pipeline/Consumer Group
```

### Design Decisions

1. **Pure Functions**: Generation functions are stateless and side-effect-free
   - Easy to test
   - Reproducible results with same seed
   - Thread-safe

2. **Domain Model First**: All events use `domain.SensorEvent`
   - Consistent across all layers
   - Rich metadata (Tags, EventID)
   - JSON serializable for Kafka

3. **Configurable Types**: Support multiple sensor types
   - Temperature, Humidity, Pressure, Moisture
   - Extensible for new sensor types

4. **Realistic Values**: Generated values within realistic ranges
   - Temperature: -10°C to 50°C
   - Humidity: 0% to 100%
   - Prevents invalid data in testing

### Integration Points

Event generation integrates with:
- **Examples**: Generate events for demonstrations
- **Tests**: Generate mock events for testing
- **Development**: Generate sample data for local development
- **Load Testing**: Generate events at scale for performance testing

---

---

## Architecture Principles

### 1. Clean Architecture

The system follows **Clean Architecture** with strict layer separation:

```
┌─────────────────────────────────────────────────────────┐
│                   External Layer                        │
│  (Examples, Main, Configuration, Infrastructure)        │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                  Use Case Layer                         │
│  (Business Logic: ConsumerGroup, Pipeline, Ingest)      │
│  - Depends on: Domain (interfaces)                      │
│  - Independent of: Infrastructure                       │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                   Domain Layer                          │
│  (Pure Entities: SensorEvent, Prediction, Alert)        │
│  - No dependencies on external frameworks               │
│  - Pure business models                                 │
└─────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────────────────────────────────────┐
│              Infrastructure Layer                       │
│  (Adapters: KafkaPublisher, DynamoDBStorage)           │
│  - Implements interfaces defined in Domain/UseCase     │
│  - Handles external system communication               │
└─────────────────────────────────────────────────────────┘
```

**Key Benefits:**
- **Testability**: Easy to mock dependencies
- **Flexibility**: Swap implementations without changing business logic
- **Independence**: Business logic independent of frameworks
- **Maintainability**: Clear boundaries and responsibilities

### 2. Dependency Inversion Principle

**High-level modules don't depend on low-level modules. Both depend on abstractions.**

```
Use Case (High-Level)          Infrastructure (Low-Level)
     │                                  │
     │                                  │
     └──────────→ Interface ←──────────┘
            (SensorIngester)
```

**Example:**
- `ConsumerGroupUseCase` depends on `SensorIngester` interface (abstraction)
- `KafkaDLQIngester` implements `SensorIngester` interface (concrete implementation)
- Business logic doesn't know about Kafka - only the interface contract

---

## Layer-by-Layer Architecture

### 1. Domain Layer (`internal/domain/`)

**Purpose**: Pure business entities with zero infrastructure dependencies

**Key Entities:**

#### `SensorEvent`
```go
type SensorEvent struct {
    EventID    string            // Unique event identifier
    SensorID   string            // Sensor identifier
    SensorType string            // Type: temperature, humidity, etc.
    Value      float64           // Measured value
    Unit       string            // Measurement unit
    Timestamp  time.Time         // When reading was taken
    Tags       map[string]string // Metadata (location, zone, etc.)
}
```

**Design Decisions:**
- **Tags as `map[string]string`**: O(1) lookup, natural key-value semantics
- **Immutable by design**: Represents point-in-time observation
- **JSON serializable**: Works across Kafka, REST APIs, databases

#### Other Domain Models
- `Prediction`: ML model inference output
- `Alert`: Anomaly detection events
- `Metrics`: System performance metrics

### 2. Use Case Layer (`internal/usecase/`)

**Purpose**: Business logic orchestration and workflow management

#### `IngestSensorUseCase` (Single Event Processing)

**Responsibilities:**
- Validate sensor events
- Call `SensorIngester` interface to persist/publish
- Handle errors gracefully (log, don't crash)

**Flow:**
```
Event → Validate → SensorIngester.Ingest() → Success/Error
```

#### `ConsumerGroupUseCase` (Kafka-like Consumer Group)
Or sign into a new workspace here
**Responsibilities:**
- Partition-based event routing
- Concurrent processing with worker pools
- Retry mechanism with exponential backoff
- DLQ handling for failed events
- Backpressure management

**Key Methods:**
- `Start(ctx)`: Initializes partitions, workers, and DLQ consumer
- `Ingest(event)`: Routes event to appropriate partition
- `Shutdown()`: Graceful shutdown with resource cleanup

#### `IngestPipelineUseCase` (Simple Worker Pool)

**Responsibilities:**
- Simple concurrent event processing
- Worker pool pattern
- Buffered channel for event queuing

### 3. Infrastructure Layer (`internal/generator/`)

**Purpose**: Adapters that implement interfaces defined in Use Case layer

#### `KafkaLocalPublisher`
- Implements `Publisher` interface
- Publishes `SensorData` to Kafka topics
- Handles connection pooling, retries, error handling
- Works with local Kafka (Docker) and production Kafka (AWS MSK)

#### `KafkaDLQIngester`
- Implements `SensorIngester` interface
- Publishes failed `SensorEvent` to Kafka DLQ topic
- Serializes events as JSON
- Uses SensorID as message key (partition routing)

#### `DynamoLocalStorage`
- Implements `Storage` interface
- Stores sensor readings in DynamoDB
- Works with LocalStack (local) and real AWS DynamoDB

---

## Consumer Group Implementation

### Architecture Overview

The Consumer Group simulates Kafka Consumer Group behavior using pure Go primitives:

```
                    Event Input
                        │
                        ↓
            ┌───────────────────────┐
            │  partitionRouting()   │
            │  (Hash SensorID)      │
            └───────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ↓               ↓               ↓
   Partition 0    Partition 1    Partition 2
   (Channel)      (Channel)      (Channel)
        │               │               │
        ├───────┬───────┼───────┬───────┤
        ↓       ↓       ↓       ↓       ↓
    Worker  Worker  Worker  Worker  Worker
        │       │       │       │       │
        └───────┴───────┴───────┴───────┘
                        │
                        ↓
            ┌───────────────────────┐
            │  processWithRetry()   │
            │  (Exponential Backoff)│
            └───────────────────────┘
                        │
            ┌───────────┴───────────┐
            │                       │
         Success              Max Retries
            │                       │
            │                   ┌───┴────┐
            │                   │  DLQ   │
            │                   │Channel │
            │                   └───┬────┘
            │                       │
            │                   ┌───┴────────┐
            │                   │dlqConsumer()│
            │                   │            │
            │                   └───┬────────┘
            │                       │
            └───────────────────────┘
                        │
                        ↓
                Kafka DLQ Topic
```

### Key Components

#### 1. Partition Routing

**Hash-based routing ensures:**
- Same SensorID → Same Partition (ordering guarantee)
- Even distribution across partitions
- Deterministic routing (same input = same partition)

```go
func routeToPartition(event SensorEvent) int {
    hash := hashString(event.SensorID)
    partitionID := hash % partitionCount
    return partitionID
}
```

**Benefits:**
- **Ordering per partition**: Events from same sensor processed in order
- **Parallelism**: Different partitions processed concurrently
- **Scalability**: Add more partitions to increase throughput

#### 2. Worker Pools

**Each partition has multiple workers:**
- Events within partition processed sequentially (per worker)
- Multiple workers per partition increase throughput
- Workers process events independently

**Configuration:**
- `PartitionCount`: Number of partitions (default: 3)
- `WorkersPerPartition`: Workers per partition (default: 2)
- Total workers = `PartitionCount × WorkersPerPartition`

#### 3. Retry Mechanism

**Exponential Backoff:**
```
Attempt 0: Immediate
Attempt 1: baseDelay × 2^0 = baseDelay (100ms)
Attempt 2: baseDelay × 2^1 = baseDelay × 2 (200ms)
Attempt 3: baseDelay × 2^2 = baseDelay × 4 (400ms)
...
```

**Configuration:**
- `MaxRetries`: Maximum retry attempts (default: 3)
- Total attempts = `MaxRetries + 1`

**Why Exponential Backoff?**
- Prevents overwhelming downstream systems
- Gives transient failures time to recover
- Standard pattern in distributed systems

#### 4. Backpressure Handling

**Two modes:**

1. **Blocking Mode** (default):
   - Waits for channel space
   - Guarantees event delivery
   - May slow down producer if consumers are slow

2. **Dropping Mode**:
   - Drops events if channel is full
   - Higher throughput
   - Risk of data loss

**Configuration:**
- `BackpressureMode`: "blocking" or "dropping"

---

## Dead Letter Queue (DLQ) Architecture

### Overview

The DLQ handles events that fail after exhausting all retry attempts. This prevents:
- Infinite retry loops
- Resource exhaustion
- Lost events (events are persisted for later analysis)

### Architecture Flow

```
Event Processing Fails
        │
        ↓
Max Retries Exceeded
        │
        ↓
┌───────────────────────┐
│   sendToDLQ(event)    │
│   (Non-blocking)      │
└───────────────────────┘
        │
        ↓
┌───────────────────────┐
│   DLQ Channel         │
│   (Buffered)          │
└───────────────────────┘
        │
        ↓
┌───────────────────────┐
│   dlqConsumer()       │
│   (Separate goroutine)│
└───────────────────────┘
        │
        ├───────────────────┐
        │                   │
        ↓                   ↓
┌───────────────┐   ┌───────────────┐
│DLQ Ingester   │   │  Log Only     │
│(Optional)     │   │(Fallback)     │
└───────────────┘   └───────────────┘
        │
        ↓
┌───────────────────────┐
│   Kafka DLQ Topic     │
│   (sensor-data-dlq)   │
└───────────────────────┘
```

### Components

#### 1. DLQ Channel

**In-memory channel for failed events:**
- Buffered channel (configurable size)
- Non-blocking send (if full, event is logged and lost)
- Thread-safe communication between workers and DLQ consumer

#### 2. DLQ Consumer

**Separate goroutine that consumes from DLQ channel:**
- Runs independently of partition workers
- Processes failed events sequentially
- Calls `dlqIngester.Ingest()` if configured
- Falls back to logging if no DLQ ingester provided

#### 3. KafkaDLQIngester

**Adapter that publishes failed events to Kafka DLQ topic:**

```go
type KafkaDLQIngester struct {
    writer *kafka.Writer  // Kafka writer for DLQ topic
    topic  string         // DLQ topic name
}

func (k *KafkaDLQIngester) Ingest(ctx context.Context, event domain.SensorEvent) error {
    // Serialize event to JSON
    jsonData, _ := json.Marshal(event)
    
    // Create Kafka message
    message := kafka.Message{
        Key:   []byte(event.SensorID),
        Value: jsonData,
    }
    
    // Publish to DLQ topic
    return k.writer.WriteMessages(ctx, message)
}
```

**Key Features:**
- Implements `SensorIngester` interface (consistent with normal ingestion)
- Publishes `SensorEvent` as JSON (preserves all event data)
- Uses SensorID as message key (enables partition routing if needed)
- Error handling: Logs errors but doesn't crash system

### Configuration

**Environment Variables:**
- `KAFKA_DLQ_TOPIC`: DLQ topic name (default: "sensor-data-dlq")
- `KAFKA_BROKER`: Kafka broker address (default: "localhost:9092")

**Consumer Group Config:**
- `DLQBufferSize`: DLQ channel buffer size (default: 50)

### Design Decisions

1. **Optional DLQ Publisher**: System works without DLQ ingester (backward compatible)
2. **Separate Topic**: Failed events go to different topic for isolation
3. **Same Interface**: DLQ uses `SensorIngester` interface (consistency)
4. **Non-blocking**: DLQ failures don't crash the system

---

## Data Flow

### Complete Event Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                   1. Event Generation                        │
│                   generateSensorEvent()                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   2. Event Submission                        │
│              consumerGroup.Ingest(event)                     │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   3. Partition Routing                       │
│         routeToPartition(event) → Partition N                │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   4. Event Queuing                           │
│         partition[N] channel (buffered)                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   5. Worker Processing                       │
│         partitionWorker() reads from channel                 │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   6. Retry Logic                             │
│         processWithRetry() with exponential backoff          │
│                                                              │
│    Attempt 0 → Attempt 1 → Attempt 2 → ... → Max Retries    │
└───────────────────────────┬─────────────────────────────────┘
                            │
            ┌───────────────┴───────────────┐
            │                               │
         Success                      Max Retries
            │                               │
            │                           ┌───┴──────────┐
            │                           │sendToDLQ()   │
            │                           └───┬──────────┘
            │                               │
            │                           ┌───┴──────────┐
            │                           │DLQ Channel   │
            │                           └───┬──────────┘
            │                               │
            │                           ┌───┴──────────┐
            │                           │dlqConsumer() │
            │                           └───┬──────────┘
            │                               │
            │                           ┌───┴──────────────┐
            │                           │KafkaDLQIngester  │
            │                           │  .Ingest()       │
            │                           └───┬──────────────┘
            │                               │
            └───────────────────────────────┘
                            │
                            ↓
            ┌───────────────────────────────┐
            │     Kafka DLQ Topic           │
            │   (sensor-data-dlq)           │
            └───────────────────────────────┘
```

### Normal Flow (Success)

1. Event generated → Submitted to consumer group
2. Routed to partition based on SensorID hash
3. Queued in partition channel
4. Worker picks up event
5. `processWithRetry()` calls `singleUseCase.Execute()`
6. `Execute()` validates and calls `ingester.Ingest()`
7. Event successfully processed

### Failure Flow (Retries + DLQ)

1. Event generated → Submitted to consumer group
2. Routed to partition → Queued → Worker picks up
3. `processWithRetry()` attempts processing
4. **First attempt fails** → Wait 100ms → Retry
5. **Second attempt fails** → Wait 200ms → Retry
6. **Third attempt fails** → Wait 400ms → Retry
7. **Max retries exceeded** → `sendToDLQ(event)`
8. Event sent to DLQ channel
9. `dlqConsumer()` reads from DLQ channel
10. `dlqIngester.Ingest()` publishes to Kafka DLQ topic

---

## Design Patterns

### 1. Worker Pool Pattern

**Purpose**: Control concurrency and resource usage

```go
// Create worker pool
for i := 0; i < workerCount; i++ {
    go worker(workChannel)
}

// Worker function
func worker(ch <-chan Work) {
    for work := range ch {
        process(work)
    }
}
```

**Benefits:**
- Limits concurrent goroutines
- Prevents resource exhaustion
- Easy to scale (increase worker count)

### 2. Producer-Consumer Pattern

**Purpose**: Decouple event generation from processing

```
Producer (Event Generator)
    ↓ (channel)
Consumer (Worker Pool)
```

**Benefits:**
- Asynchronous processing
- Backpressure handling (bounded channels)
- Independent scaling

### 3. Adapter Pattern

**Purpose**: Bridge between interfaces and implementations

```go
// Interface (Port)
type SensorIngester interface {
    Ingest(ctx context.Context, event SensorEvent) error
}

// Adapters (Implementations)
- KafkaDLQIngester: Publishes to Kafka DLQ topic
- MockIngester: Captures events for testing
- DynamoDBIngester: Saves to DynamoDB
```

**Benefits:**
- Swappable implementations
- Easy testing with mocks
- Gradual migration between systems

### 4. Strategy Pattern

**Purpose**: Selectable algorithms (backpressure modes)

```go
if config.BackpressureMode == "dropping" {
    // Non-blocking send, drop if full
} else {
    // Blocking send, wait for space
}
```

### 5. Observer Pattern

**Purpose**: Event-driven processing

- DLQ consumer observes DLQ channel
- Workers observe partition channels
- All observe context cancellation

---

## Key Components

### ConsumerGroupUseCase

**Responsibilities:**
- Partition management
- Worker pool orchestration
- Retry logic coordination
- DLQ handling
- Graceful shutdown

**Key Fields:**
```go
type ConsumerGroupUseCase struct {
    config        *ConsumerGroupConfig
    singleUseCase *IngestSensorUseCase
    logger        Logger
    dlqIngester   SensorIngester  // Optional DLQ publisher
    partitions    []chan SensorEvent
    dlq           chan SensorEvent
    wg            sync.WaitGroup
    ctx           context.Context
    cancel        context.CancelFunc
}
```

### IngestSensorUseCase

**Responsibilities:**
- Event validation
- Delegation to `SensorIngester`
- Error handling

**Flow:**
```
Validate → SensorIngester.Ingest() → Success/Error
```

### KafkaDLQIngester

**Responsibilities:**
- Serialize `SensorEvent` to JSON
- Publish to Kafka DLQ topic
- Error handling

**Key Methods:**
- `Ingest(ctx, event)`: Publishes event to DLQ topic
- `Close()`: Closes Kafka writer

---

## Error Handling & Resilience

### Error Handling Strategy

1. **Validation Errors**: Log and return error (event is invalid)
2. **Ingestion Errors**: Log and retry (transient failures)
3. **Max Retries Exceeded**: Send to DLQ (permanent failures)
4. **DLQ Publishing Errors**: Log but don't crash (system continues)

### Resilience Mechanisms

1. **Retry with Exponential Backoff**
   - Handles transient failures
   - Prevents overwhelming downstream systems

2. **DLQ for Permanent Failures**
   - Prevents infinite retry loops
   - Enables later analysis and reprocessing

3. **Graceful Shutdown**
   - Context cancellation propagation
   - Wait for workers to finish
   - Close channels and connections

4. **Backpressure Handling**
   - Prevents memory exhaustion
   - Provides flow control

5. **Error Isolation**
   - Errors in one event don't affect others
   - Errors in one partition don't affect others
   - DLQ failures don't crash the system

---

## Scalability & Performance

### Horizontal Scalability

**Add more partitions:**
- More partitions → More parallelism
- Each partition processes independently
- No coordination needed between partitions

**Add more workers per partition:**
- More workers → Higher throughput per partition
- Workers process events concurrently within partition

**Add more consumer group instances:**
- Multiple processes/machines
- Each instance handles subset of partitions
- Total throughput = instances × partitions × workers

### Vertical Scalability

**Increase buffer sizes:**
- Larger channel buffers → Handle more bursts
- Trade-off: More memory usage

**Increase worker count:**
- More workers → Higher throughput
- Trade-off: More CPU usage

### Performance Characteristics

**Partitioning:**
- O(1) partition routing (hash computation)
- Parallel processing across partitions
- No shared state (no locks needed)

**Retry Logic:**
- Exponential backoff prevents thundering herd
- Configurable max retries
- Context-aware cancellation

**DLQ Processing:**
- Asynchronous (separate goroutine)
- Non-blocking (buffered channel)
- Independent of main processing

---

## Configuration

### Environment Variables

**Kafka:**
```bash
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=sensor-data
KAFKA_DLQ_TOPIC=sensor-data-dlq
```

**Consumer Group:**
```bash
PARTITION_COUNT=3
WORKERS_PER_PARTITION=2
MAX_RETRIES=3
DLQ_BUFFER_SIZE=50
BACKPRESSURE_MODE=blocking
```

### Configuration Loading

**Strategy:**
- Environment variables with sensible defaults
- Same code works in dev/staging/prod
- Only configuration changes needed

---

## Pure Functions in Go

### What are Pure Functions?

A **pure function** is a function that:
1. **Always returns the same output for the same input** (deterministic)
2. **Has no side effects** (doesn't modify global state, doesn't perform I/O)
3. **Doesn't depend on external mutable state** (only uses its parameters)

### Why Pure Functions Matter

**Benefits:**
- **Testability**: Easy to test - same input always produces same output
- **Predictability**: No hidden state changes or side effects
- **Thread-Safety**: Safe to call from multiple goroutines (no shared mutable state)
- **Composability**: Easy to combine and compose functions
- **Reasoning**: Easier to understand and reason about code
- **Debugging**: No state changes make debugging simpler

### Pure Functions in This Codebase

#### Example 1: `GenerateSensorEvent()` (Pure Function)

**Location**: `internal/generator/functions.go`

```go
func GenerateSensorEvent(sensorType string, sensorNumber int, eventIndex int) domain.SensorEvent {
    // 1. No side effects - doesn't modify global state
    // 2. Deterministic - same inputs produce same outputs (if random seed is fixed)
    // 3. Only uses parameters - no external mutable state
    
    var config SensorConfig
    switch sensorType {
    case SensorTypeTemperature:
        config = TemperatureConfig
    // ...
    }
    
    return domain.SensorEvent{
        EventID:    fmt.Sprintf("event-%03d", eventIndex),
        SensorID:   config.GenerateSensorID(sensorNumber),
        // ...
    }
}
```

**Why this is pure:**
- ✅ Takes input parameters only
- ✅ Returns new value (doesn't modify inputs)
- ✅ No I/O operations
- ✅ No global state modification
- ✅ Same inputs → same outputs (deterministic)

**Note**: If it uses `rand`, it's technically impure, but in practice it's treated as pure because:
- Randomness is a deliberate design choice
- Function signature clearly indicates what it does
- Still testable with seed control

#### Example 2: `routeToPartition()` (Pure Function)

**Location**: `internal/usecase/consumer_group.go`

```go
func (cg *ConsumerGroupUseCase) routeToPartition(event domain.SensorEvent) int {
    hash := hashString(event.SensorID)
    partitionID := hash % cg.config.PartitionCount
    if partitionID < 0 {
        partitionID = -partitionID
    }
    return partitionID
}

func hashString(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32())
}
```

**Why this is pure:**
- ✅ Deterministic: Same SensorID always routes to same partition
- ✅ No side effects: Only computes and returns value
- ✅ No I/O: Pure computation
- ✅ Thread-safe: No shared mutable state

#### Example 3: `validateEvent()` (Pure Function)

**Location**: `internal/usecase/ingest_sensor.go`

```go
func (uc *IngestSensorUseCase) validateEvent(event domain.SensorEvent) error {
    // Pure validation logic
    if event.EventID == "" {
        return fmt.Errorf("event ID is required")
    }
    // ... more validation
    return nil
}
```

**Why this is pure:**
- ✅ Only checks input parameters
- ✅ Returns result without modifying input
- ✅ No side effects
- ✅ Deterministic

### Impure Functions (For Comparison)

#### Example: `dlqConsumer()` (Impure Function)

**Location**: `internal/usecase/consumer_group.go`

```go
func (cg *ConsumerGroupUseCase) dlqConsumer() {
    for {
        select {
        case event, ok := <-cg.dlq:
            // Side effect: Reads from channel (I/O)
            // Side effect: Calls dlqIngester.Ingest() (Kafka publish)
            // Side effect: Modifies logger state (I/O)
            if err := cg.dlqIngester.Ingest(cg.ctx, event); err != nil {
                cg.logger.Error(...) // Side effect
            }
        }
    }
}
```

**Why this is impure:**
- ❌ Side effects: Reads from channel, publishes to Kafka, logs
- ❌ I/O operations: Network calls, logging
- ❌ Not deterministic: Behavior depends on channel state, network conditions

**Note**: Impure functions are necessary for:
- I/O operations (Kafka, database, logging)
- Stateful operations (channel reads, state machines)
- Side effects that are the purpose of the function

### Pure Function Patterns in Go

#### Pattern 1: Immutable Operations

```go
// Pure: Returns new slice without modifying input
func AddReading(readings []SensorData, data SensorData) []SensorData {
    if !data.IsValid() {
        return readings // Return unchanged
    }
    return append(readings, data) // New slice
}

// Pure: Returns new map without modifying input
func UpdateLatestReadings(latest map[string]SensorData, data SensorData) map[string]SensorData {
    result := make(map[string]SensorData, len(latest)+1)
    for k, v := range latest {
        result[k] = v // Copy existing entries
    }
    result[data.ID] = data // Add/update
    return result
}
```

#### Pattern 2: Computations Without Side Effects

```go
// Pure: Only computation, no side effects
func hashString(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32())
}

// Pure: Filtering without modifying input
func FilterReadingsByType(readings []SensorData, sensorType string) []SensorData {
    result := make([]SensorData, 0)
    for _, reading := range readings {
        if reading.Type == sensorType {
            result = append(result, reading)
        }
    }
    return result
}
```

### Testing Pure Functions

**Easy to test because:**
- Same input → same output
- No setup/teardown needed (no external dependencies)
- No mocking needed (no side effects)
- Fast execution (no I/O)

**Example Test:**
```go
func TestGenerateSensorEvent(t *testing.T) {
    event := generator.GenerateSensorEvent(
        generator.SensorTypeTemperature,
        1, // sensor number
        0, // event index
    )
    
    // Assertions - same inputs always produce same outputs
    assert.Equal(t, "event-000", event.EventID)
    assert.Equal(t, "temp-001", event.SensorID)
    assert.Equal(t, "temperature", event.SensorType)
}
```

### When to Use Pure Functions

**Use pure functions for:**
- ✅ Data transformation
- ✅ Validation logic
- ✅ Computations
- ✅ Filtering/sorting
- ✅ Data generation (with controlled randomness)
- ✅ Business logic calculations

**Use impure functions for:**
- I/O operations (Kafka, database, HTTP)
- State management (channels, shared state)
- Logging
- Network operations
- File operations

### Pure Functions vs Methods

**Pure Function** (standalone):
```go
func GenerateSensorEvent(...) domain.SensorEvent {
    // No receiver, pure computation
}
```

**Method** (on a struct, may be impure):
```go
func (cg *ConsumerGroupUseCase) routeToPartition(...) int {
    // Uses receiver, but still pure (no side effects)
}
```

**Key Point**: A method can be pure if it doesn't modify the receiver's state and has no side effects.

### Summary: Pure Functions in This Codebase

**Pure Functions:**
- `GenerateSensorEvent()` - Event generation
- `GenerateSensorEvents()` - Batch generation
- `routeToPartition()` - Partition routing
- `hashString()` - Hash computation
- `validateEvent()` - Validation logic
- `AddReading()` - Immutable slice operations
- `FilterReadingsByType()` - Filtering operations

**Impure Functions** (necessary for I/O and side effects):
- `dlqConsumer()` - Reads from channel, publishes to Kafka
- `partitionWorker()` - Reads from channel, processes events
- `KafkaDLQIngester.Ingest()` - Publishes to Kafka (I/O)
- `Logger.Error()` - Logs messages (I/O)

### Benefits Realized in This Codebase

1. **Testability**: Pure functions are easily unit tested
2. **Concurrency Safety**: Pure functions are thread-safe by design
3. **Composability**: Easy to combine pure functions
4. **Debugging**: Easier to reason about and debug
5. **Performance**: Pure functions can be optimized/cached

---

## Testing Strategy

### Unit Testing

- **Domain Models**: Pure functions, easy to test
- **Use Cases**: Mock `SensorIngester` interface
- **Infrastructure**: Test with LocalStack/local Kafka

### Integration Testing

- Test with real Kafka (Docker)
- Test with LocalStack (DynamoDB)
- Test DLQ publishing end-to-end

### Mock Implementations

- `MockIngester`: Simple mock that captures events
- `FailingMockIngester`: Randomly fails to test retry/DLQ
- In-memory implementations for testing

---

## Future Enhancements

1. **Metrics & Monitoring**
   - Prometheus metrics (event counts, latencies)
   - DLQ size monitoring
   - Partition lag tracking

2. **DLQ Reprocessing**
   - Tool to reprocess DLQ events
   - Filtering and transformation
   - Dead letter queue dashboard

3. **Advanced Retry**
   - Configurable retry strategies
   - Circuit breaker pattern
   - Retry topic (before DLQ)

4. **Distributed Tracing**
   - Trace event flow across partitions
   - Debug slow events
   - Performance profiling

---

## Summary

This architecture demonstrates:

✅ **Clean Architecture**: Clear layer separation, testable code  
✅ **Dependency Inversion**: Interface-based design  
✅ **Resilience**: Retry mechanisms, DLQ handling  
✅ **Scalability**: Partition-based parallelism  
✅ **Production-Ready**: Error handling, graceful shutdown, backpressure  

The DLQ implementation follows the same interface pattern as normal ingestion, ensuring consistency and allowing easy testing. Failed events are persisted to Kafka DLQ topic, enabling later analysis and reprocessing without losing data.
