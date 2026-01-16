# Generator/Adapter Layer

Infrastructure adapters and implementations for Kafka, DynamoDB, and in-memory storage. This layer implements the interfaces (ports) defined in the use case layer.

## Architecture

This package contains:
- **Interfaces**: Contracts for generators, publishers, and storage
- **Implementations**: Concrete adapters (Kafka, DynamoDB, in-memory)
- **Configuration**: Environment-based settings
- **Data Models**: Sensor data structures

**Dependencies:**
- ✅ Domain models (for data structures)
- ✅ Infrastructure libraries (Kafka, AWS SDK)
- ❌ Does NOT depend on use case layer (dependency inversion)

## Files

### `interfaces.go` - Service Contracts
Defines interfaces that implementations must satisfy.

**Interfaces:**
1. **SensorGenerator**: Contract for generating sensor data
   - `Generate() SensorData`
   - Implementations: RandomSensorGenerator, RealSensorGenerator

2. **Publisher**: Contract for publishing to message queue
   - `Publish(ctx, data) error`
   - `PublishBatch(ctx, data[]) error`
   - Implementations: InMemoryPublisher, KafkaPublisher

3. **Storage**: Contract for persisting sensor data
   - `Save(ctx, data) error`
   - `SaveBatch(ctx, data[]) error`
   - `GetLatest(ctx, sensorID) (SensorData, bool, error)`
   - Implementations: InMemoryStorage, DynamoDBStorage

**Why Interfaces?**
- Decoupling: Services depend on abstractions
- Testability: Easy to create mocks
- Flexibility: Swap implementations
- Scalability: Different services can implement differently

### `sensor.go` - Sensor Data Model
Core data structure for sensor readings.

**Struct:**
```go
type SensorData struct {
    ID        string
    Type      string
    Value     float64
    Timestamp time.Time
    Unit      string
}
```

**Constants:**
- Sensor types: Temperature, Humidity, Moisture, Pressure, Light
- Units: Celsius, Fahrenheit, Percent, Pascal, Lux

**Configurations:**
- Predefined configs for each sensor type (min/max values, units)

**Methods:**
- `String()`: Human-readable representation
- `IsValid()`: Basic validation

**Usage:**
- Flows through all services
- Used as partition key in DynamoDB
- Serialized for Kafka messages

### `config.go` - Configuration
Loads Kafka and DynamoDB configuration from environment variables.

**KafkaConfig:**
- `Broker`: Kafka broker address
- `Topic`: Kafka topic name
- Loaded from: `KAFKA_BROKER`, `KAFKA_TOPIC`

**DynamoConfig:**
- `Endpoint`: DynamoDB endpoint (empty for real AWS)
- `Region`: AWS region
- `Table`: DynamoDB table name
- Loaded from: `DYNAMO_ENDPOINT`, `AWS_REGION`, `SENSOR_TABLE`

**Why Environment Variables?**
- Different environments need different endpoints
- Local: localhost:9092, http://localhost:4566
- Production: AWS MSK, real DynamoDB
- No code changes needed, only config changes

### `implementations.go` - In-Memory Implementations
Development/testing implementations.

**RandomSensorGenerator:**
- Generates random sensor readings
- Used for simulation and testing
- In production: replaced with real hardware interface

**InMemoryPublisher:**
- Logs published data to console
- Used for development
- In production: replaced with KafkaPublisher

**InMemoryStorage:**
- Stores data in memory (slice and map)
- Used for development
- In production: replaced with DynamoDBStorage

**Why In-Memory?**
- Fast development iteration
- No infrastructure needed
- Easy testing
- Gradual migration path

### `kafka_publisher.go` - Kafka Adapter
Publishes sensor data to Kafka topics.

**KafkaLocalPublisher:**
- Connects to local Kafka (Docker)
- Uses segmentio/kafka-go library
- Supports single and batch publishing

**Features:**
- Connection management
- Error handling
- Context support (cancellation, timeouts)
- Batch publishing for throughput

**Configuration:**
- Broker address from environment
- Topic name from environment
- Works with local Kafka and AWS MSK (same code!)

**Usage:**
```go
config := generator.LoadKafkaConfig()
publisher, err := generator.NewKafkaLocalPublisher(config)
publisher.Publish(ctx, sensorData)
```

### `dynamo_storage.go` - DynamoDB Adapter
Stores sensor data in DynamoDB.

**DynamoLocalStorage:**
- Connects to LocalStack (local) or real AWS DynamoDB
- Uses AWS SDK v2
- Supports single and batch operations

**Table Schema:**
- Partition Key: SensorID (String)
- Sort Key: Timestamp (Number, Unix epoch)
- Attributes: Type, Value, Unit

**Features:**
- Works with LocalStack and real AWS
- Custom endpoint resolver for LocalStack
- Batch operations for efficiency
- Latest reading queries

**Configuration:**
- Endpoint from environment (empty = real AWS)
- Region from environment
- Table name from environment

**Usage:**
```go
config := generator.LoadDynamoConfig()
storage, err := generator.NewDynamoLocalStorage(config)
storage.Save(ctx, sensorData)
```

### `functions.go` - Pure Functions
Stateless helper functions for data manipulation.

**Functions:**
- `GenerateSensorData()`: Creates sensor data
- `AddReading()`: Adds reading to slice (immutability pattern)
- `UpdateLatestReadings()`: Updates latest readings map

**Why Pure Functions?**
- Stateless and easier to test
- No side effects
- Can be used with different configurations
- Follows functional programming principles

### `manager.go` - Data Manager
Manages sensor readings in memory using slices and maps.

**DataManager:**
- Maintains readings slice (for batch processing)
- Maintains latest readings map (for O(1) lookup)
- Provides batch operations

**Why Both Slice and Map?**
- **Slice**: Batch processing for Kafka (better throughput)
- **Map**: O(1) lookup for latest values (health checks, alerts)

**Methods:**
- `AddReading()`: Adds reading (updates both slice and map)
- `GetReadingsBatch()`: Gets batch and clears slice
- `GetLatestReading()`: Gets latest value for sensor (O(1))
- `GetAllLatestReadings()`: Gets all latest values

## Migration Path

### Development → Production

**Local Development:**
- InMemoryPublisher (logs to console)
- InMemoryStorage (stores in memory)
- RandomSensorGenerator (simulates data)

**Local with Infrastructure:**
- KafkaLocalPublisher (local Kafka Docker)
- DynamoLocalStorage (LocalStack)
- RandomSensorGenerator

**Production:**
- KafkaPublisher (AWS MSK)
- DynamoDBStorage (real AWS DynamoDB)
- RealSensorGenerator (hardware interface)

**Key Point:** Same code, different implementations!

## Usage Examples

### Kafka Publisher
```go
config := generator.LoadKafkaConfig()
publisher, err := generator.NewKafkaLocalPublisher(config)
defer publisher.Close()

// Single publish
publisher.Publish(ctx, sensorData)

// Batch publish
publisher.PublishBatch(ctx, []SensorData{...})
```

### DynamoDB Storage
```go
config := generator.LoadDynamoConfig()
storage, err := generator.NewDynamoLocalStorage(config)

// Save single
storage.Save(ctx, sensorData)

// Save batch
storage.SaveBatch(ctx, []SensorData{...})

// Get latest
latest, exists, err := storage.GetLatest(ctx, "sensor-001")
```

### Data Manager
```go
manager := generator.NewDataManager()

// Add readings
manager.AddReading(sensorData)

// Get batch for Kafka
batch := manager.GetReadingsBatch()

// Get latest value
latest, exists := manager.GetLatestReading("sensor-001")
```

## Testing

All implementations can be easily tested:
- In-memory implementations for fast tests
- Mock implementations for unit tests
- Integration tests with local infrastructure

## Key Design Patterns

1. **Dependency Inversion**: Implementations depend on interfaces
2. **Adapter Pattern**: Adapters bridge infrastructure and business logic
3. **Factory Pattern**: Constructors create implementations
4. **Strategy Pattern**: Different implementations for different strategies
