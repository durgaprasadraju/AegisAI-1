# Data Generator Service - AegisAI

The data-generator service is responsible for simulating sensor data and preparing it for distribution through the AegisAI platform. This service demonstrates Go fundamentals in the context of distributed microservices.

## Table of Contents

1. [Go Basics in AegisAI Context](#go-basics-in-aegisai-context)
2. [SensorData Structure](#sensordata-structure)
3. [Slices and Arrays](#slices-and-arrays)
4. [Maps](#maps)
5. [Data Manager](#data-manager)
6. [Usage Examples](#usage-examples)
7. [Integration Patterns](#integration-patterns)
8. [Running the Service](#running-the-service)

---

## Go Basics in AegisAI Context

### Variables and Constants

In Go, variables store data that can change, while constants are immutable values.

```go
// Variables - can be changed
var sensorID string = "sensor-001"
var temperature float64 = 25.5
var isActive bool = true

// Short variable declaration (most common)
id := "sensor-002"
value := 98.6

// Constants - cannot be changed
const maxReadings = 1000
const sensorPrefix = "SENSOR-"
```

**Why in AegisAI?**
- Variables store sensor readings that change over time
- Constants define configuration values (sensor types, units) that remain consistent across the distributed system

### Types

Go has several built-in types. In AegisAI, we primarily use:

- **`string`**: Sensor IDs, types, units (e.g., `"sensor-001"`, `"temperature"`)
- **`float64`**: Sensor values (e.g., `25.5` for temperature, `65.0` for humidity)
- **`bool`**: Status flags (e.g., `isActive`, `isHealthy`)
- **`time.Time`**: Timestamps for time-series data (critical for ordering and analysis)
- **`int`**: Counters, indices (e.g., reading count, sensor number)

```go
var sensorID string = "temp-001"        // Sensor identifier
var value float64 = 25.5                // Measurement value
var timestamp time.Time = time.Now()    // When reading was taken
var count int = 42                      // Number of readings
```

---

## SensorData Structure

### What is a Struct?

A **struct** is a collection of fields that group related data together. Think of it as a blueprint for creating data objects.

### SensorData Struct

The `SensorData` struct is the core data model in AegisAI. It represents a single sensor reading that flows through the entire distributed system.

```go
type SensorData struct {
    ID        string    // Sensor identifier (e.g., "sensor-001")
    Type      string    // Sensor type: "temperature", "humidity", "moisture"
    Value     float64   // The actual sensor reading value
    Timestamp time.Time // When the reading was taken
    Unit      string    // Measurement unit: "celsius", "percent", "pascal"
}
```

### Why This Structure?

Each field serves a specific purpose in the distributed system:

- **ID**: Unique identifier used as partition key in DynamoDB, Kafka message key
- **Type**: Routes data to appropriate processing logic in different services
- **Value**: The actual measurement (float64 for precision)
- **Timestamp**: Critical for time-series analysis, ordering, and time-based queries
- **Unit**: Ensures proper interpretation across microservices

### Creating SensorData Instances

```go
// Method 1: Field names (most readable)
reading := SensorData{
    ID:        "sensor-001",
    Type:      "temperature",
    Value:     25.5,
    Timestamp: time.Now(),
    Unit:      "celsius",
}

// Method 2: Positional (order matters)
reading := SensorData{
    "sensor-001",
    "temperature",
    25.5,
    time.Now(),
    "celsius",
}

// Method 3: Zero value, then set fields
var reading SensorData
reading.ID = "sensor-001"
reading.Type = "temperature"
reading.Value = 25.5
reading.Timestamp = time.Now()
reading.Unit = "celsius"
```

### Sensor Type Constants

Constants ensure type safety and prevent typos across microservices:

```go
const (
    SensorTypeTemperature = "temperature"
    SensorTypeHumidity    = "humidity"
    SensorTypeMoisture    = "moisture"
    SensorTypePressure    = "pressure"
)
```

---

## Slices and Arrays

### Arrays vs Slices

**Arrays** have a fixed size determined at compile time:
```go
var readings [5]SensorData  // Array of exactly 5 elements
readings[0] = SensorData{...}
```

**Slices** are dynamic - they can grow and shrink:
```go
var readings []SensorData           // Empty slice
readings = make([]SensorData, 0, 10) // Length 0, capacity 10
readings = []SensorData{...}        // Initialize with values
```

### Why Slices in AegisAI?

Slices are used extensively in the data-generator service for:

1. **Batch Processing**: Accumulate readings before sending to Kafka
   - Better throughput (send multiple readings in one message)
   - Lower network overhead
   - Reduced Kafka producer overhead

2. **Maintaining Order**: Slices preserve insertion order
   - Critical for time-series data
   - Ensures chronological processing

3. **Memory Efficiency**: Only allocate what you need
   - Start small, grow as needed
   - Clear after sending to Kafka

### Working with Slices

```go
// Create empty slice
readings := []SensorData{}

// Append readings (slice grows automatically)
readings = append(readings, SensorData{
    ID: "sensor-001",
    Type: "temperature",
    Value: 25.5,
    Timestamp: time.Now(),
    Unit: "celsius",
})

// Append multiple at once
readings = append(readings, reading1, reading2, reading3)

// Get length
count := len(readings)  // Number of elements

// Iterate over slice
for i, reading := range readings {
    fmt.Printf("Reading %d: %s = %.2f\n", i, reading.ID, reading.Value)
}

// Slice operations (get portion of slice)
firstTwo := readings[:2]           // First 2 elements
lastTwo := readings[len(readings)-2:]  // Last 2 elements
```

### Batch Processing Pattern

```go
// Accumulate readings in slice
for i := 0; i < 100; i++ {
    reading := generateReading()
    readings = append(readings, reading)
}

// Send entire batch to Kafka
batch := getBatch()  // Returns slice and clears internal storage
sendToKafka(batch)   // Send all at once
```

---

## Maps

### What is a Map?

A **map** is a collection of key-value pairs. It provides O(1) lookup time - instant access to values by their key.

```go
// Map syntax: map[keyType]valueType
latestReadings := make(map[string]SensorData)
// Key: string (sensor ID)
// Value: SensorData (latest reading)
```

### Why Maps in AegisAI?

Maps are perfect for storing the **latest reading per sensor**:

1. **O(1) Lookup**: Instant access to latest value for any sensor
   - No need to search through slices
   - Critical for health checks and alert evaluation

2. **Automatic Updates**: New reading for same ID replaces old one
   - Always maintains most recent value
   - No manual tracking needed

3. **Efficient Storage**: Only one entry per sensor
   - Much smaller than storing all historical readings
   - Perfect for API endpoints showing current state

### Working with Maps

```go
// Create map
latestReadings := make(map[string]SensorData)

// Add/Update entry
latestReadings["sensor-001"] = SensorData{
    ID: "sensor-001",
    Type: "temperature",
    Value: 25.5,
    Timestamp: time.Now(),
    Unit: "celsius",
}

// Access value
reading := latestReadings["sensor-001"]

// Safe access with existence check
reading, exists := latestReadings["sensor-001"]
if exists {
    fmt.Printf("Latest value: %.2f\n", reading.Value)
} else {
    fmt.Println("Sensor not found")
}

// Delete entry
delete(latestReadings, "sensor-001")

// Iterate over map
for sensorID, reading := range latestReadings {
    fmt.Printf("%s: %.2f\n", sensorID, reading.Value)
}

// Get count
count := len(latestReadings)  // Number of unique sensors
```

### Map Use Cases in AegisAI

1. **Health Checks**: Quickly check if sensor is reporting
   ```go
   latest, exists := manager.GetLatestReading("sensor-001")
   if !exists || time.Since(latest.Timestamp) > 5*time.Minute {
       // Sensor is down or stale
   }
   ```

2. **Alert Evaluation**: Check latest value against threshold
   ```go
   latest, _ := manager.GetLatestReading("temp-001")
   if latest.Value > 30.0 {
       triggerAlert("Temperature too high")
   }
   ```

3. **API Endpoints**: Expose current sensor state
   ```go
   allLatest := manager.GetAllLatestReadings()
   // Return as JSON for REST API
   ```

---

## Data Manager

The `DataManager` is the core component that manages sensor readings using both slices and maps.

### Why Both Slices AND Maps?

- **Slices**: For batch operations (sending to Kafka)
- **Maps**: For quick lookups (health checks, alerts, APIs)

They work together:
- When you add a reading, it goes into both the slice (for batching) and the map (for lookups)
- After sending batch to Kafka, the slice is cleared but the map remains (for quick access)

### DataManager Structure

```go
type DataManager struct {
    readings       []SensorData        // Slice for batch processing
    latestReadings map[string]SensorData // Map for O(1) lookups
    mu             sync.RWMutex        // Thread-safe access
}
```

### Key Operations

#### Add Reading

```go
manager := generator.NewDataManager()

reading := SensorData{
    ID: "sensor-001",
    Type: "temperature",
    Value: 25.5,
    Timestamp: time.Now(),
    Unit: "celsius",
}

err := manager.AddReading(reading)
// This adds to both:
// - Slice (for batch processing)
// - Map (for quick lookups)
```

#### Get Batch (for Kafka)

```go
// Get all accumulated readings
batch := manager.GetReadingsBatch()
// This:
// 1. Returns a copy of all readings
// 2. Clears the internal slice (ready for next batch)
// 3. Keeps the map intact (for lookups)

// Send to Kafka
for _, reading := range batch {
    sendToKafka(reading)
}
```

#### Get Latest Reading

```go
// O(1) lookup - instant access
latest, exists := manager.GetLatestReading("sensor-001")
if exists {
    fmt.Printf("Latest: %.2f\n", latest.Value)
}
```

#### Get All Latest Readings

```go
// Get map of all latest readings
allLatest := manager.GetAllLatestReadings()
// Useful for:
// - REST API endpoints
// - Monitoring dashboards
// - Health check reports
```

---

## Usage Examples

### Example 1: Create 5 Sensor Readings

```go
package main

import (
    "fmt"
    "time"
    "github.com/aegisai/data-generator/internal/generator"
)

func main() {
    // Initialize manager
    manager := generator.NewDataManager()

    // Generate 5 different sensor readings
    readings := []generator.SensorData{
        generator.GenerateRandomReading(generator.TemperatureConfig, 1),
        generator.GenerateRandomReading(generator.TemperatureConfig, 2),
        generator.GenerateRandomReading(generator.HumidityConfig, 1),
        generator.GenerateRandomReading(generator.MoistureConfig, 1),
        generator.GenerateRandomReading(generator.PressureConfig, 1),
    }

    // Add all readings to manager
    for _, reading := range readings {
        manager.AddReading(reading)
        fmt.Printf("Added: %s\n", reading.String())
    }

    // Show latest values from map
    fmt.Println("\nLatest readings by sensor ID:")
    latest := manager.GetAllLatestReadings()
    for sensorID, reading := range latest {
        fmt.Printf("  %s: %.2f %s\n", sensorID, reading.Value, reading.Unit)
    }
}
```

### Example 2: Continuous Data Generation

```go
func generateContinuousReadings(manager *generator.DataManager, ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second) // Generate every 5 seconds
    defer ticker.Stop()

    sensorNumber := 1
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Generate reading
            reading := generator.GenerateRandomReading(
                generator.TemperatureConfig,
                sensorNumber,
            )

            // Add to manager
            manager.AddReading(reading)

            // If batch is large enough, send to Kafka
            if manager.GetReadingsCount() >= 10 {
                batch := manager.GetReadingsBatch()
                sendBatchToKafka(batch)
            }
        }
    }
}
```

### Example 3: Building Latest Values Map

```go
func buildLatestValuesMap(readings []generator.SensorData) map[string]generator.SensorData {
    latestMap := make(map[string]generator.SensorData)

    for _, reading := range readings {
        existing, exists := latestMap[reading.ID]

        if !exists {
            // First reading for this sensor
            latestMap[reading.ID] = reading
        } else {
            // Keep the one with the latest timestamp
            if reading.Timestamp.After(existing.Timestamp) {
                latestMap[reading.ID] = reading
            }
        }
    }

    return latestMap
}
```

---

## Integration Patterns

### Kafka Integration

The data-generator service sends batches of readings to Kafka:

```go
// 1. Accumulate readings in slice
for i := 0; i < 100; i++ {
    reading := generateReading()
    manager.AddReading(reading)
}

// 2. Get batch when ready
batch := manager.GetReadingsBatch()

// 3. Serialize and send to Kafka
for _, reading := range batch {
    // Serialize to JSON
    jsonData, _ := json.Marshal(reading)

    // Send to Kafka topic
    // Key: sensor ID (ensures ordering per sensor)
    // Value: JSON serialized reading
    kafkaProducer.Send("sensor-data", reading.ID, jsonData)
}
```

**Why batch?**
- Better throughput (multiple readings per message)
- Lower network overhead
- Reduced Kafka producer overhead
- Better resource utilization

**Kafka Topic Structure:**
- **Topic**: `sensor-data`
- **Partition Key**: Sensor ID (ensures ordering per sensor)
- **Value**: JSON/Protobuf serialized SensorData

### DynamoDB Integration

The ingestion-service consumes from Kafka and stores in DynamoDB:

#### Time-Series Table

Store all historical readings:

```go
// Table: sensor-readings
// Partition Key: SensorID (string)
// Sort Key: Timestamp (number - Unix timestamp)

for _, reading := range batch {
    dynamodb.PutItem("sensor-readings", map[string]interface{}{
        "SensorID":  reading.ID,
        "Timestamp": reading.Timestamp.Unix(),
        "Type":      reading.Type,
        "Value":     reading.Value,
        "Unit":      reading.Unit,
    })
}
```

**Query Pattern:**
```go
// Get all readings for a sensor in time range
query := dynamodb.Query("sensor-readings", map[string]interface{}{
    "SensorID": "sensor-001",
    "Timestamp": map[string]int64{
        "between": [2]int64{startTime, endTime},
    },
})
```

#### Latest Values Table

Store only the most recent reading per sensor:

```go
// Table: sensor-latest
// Partition Key: SensorID (string)
// No sort key (single item per sensor)

for sensorID, reading := range latestReadings {
    dynamodb.PutItem("sensor-latest", map[string]interface{}{
        "SensorID":  reading.ID,
        "Type":      reading.Type,
        "Value":     reading.Value,
        "Unit":      reading.Unit,
        "Timestamp": reading.Timestamp.Unix(),
    })
}
```

**Query Pattern:**
```go
// Get latest value for a sensor (O(1) lookup)
latest := dynamodb.GetItem("sensor-latest", map[string]string{
    "SensorID": "sensor-001",
})
```

### Service Integration Flow

```
┌─────────────────┐
│ data-generator  │
│                 │
│ 1. Generate     │
│ 2. Add to slice │
│ 3. Update map   │
│ 4. Batch send   │
└────────┬────────┘
         │ Kafka Topic: sensor-data
         ▼
┌─────────────────┐
│ingestion-service│
│                 │
│ 1. Consume      │
│ 2. Store TS     │
│ 3. Update latest│
└────────┬────────┘
         │ DynamoDB
         ▼
┌─────────────────┐
│   ml-service    │
│                 │
│ 1. Query latest │
│ 2. Inference    │
│ 3. Return       │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ alert-service   │
│                 │
│ 1. Monitor map  │
│ 2. Evaluate     │
│ 3. Trigger      │
└─────────────────┘
```

---

## Running the Service

### Prerequisites

- Go 1.21 or later
- No external dependencies required for foundation

### Run the Example

```bash
cd services/data-generator
go run cmd/main.go
```

This will demonstrate:
- Creating 5 sensor readings
- Storing them in a slice
- Building a map of latest values
- Batch processing pattern
- O(1) lookup examples

### Build the Service

```bash
cd services/data-generator
go build -o bin/data-generator ./cmd
./bin/data-generator
```

### Run Tests

```bash
cd services/data-generator
go test ./...
```

---

## Key Takeaways

### Data Structures Choice

| Structure | Use Case | Why |
|-----------|----------|-----|
| **Slice** | Batch processing | Maintains order, efficient for sequential operations |
| **Map** | Latest value lookup | O(1) access, automatic updates |
| **Struct** | Data model | Type-safe, groups related data |

### Distributed Systems Patterns

1. **Batch Processing**: Accumulate in slice, send in batches to Kafka
2. **Latest Value Tracking**: Use map for O(1) lookups across services
3. **Time-Series Storage**: Store all readings in DynamoDB with timestamp sort key
4. **State Management**: Map provides current state for health checks and alerts

### Best Practices

1. **Validate Early**: Check data validity before adding to manager
2. **Batch Efficiently**: Accumulate readings before sending to Kafka
3. **Thread Safety**: Use mutex when accessing shared data structures
4. **Context Support**: Use context for cancellation and timeouts
5. **Clear After Use**: Clear slice after sending batch to prevent memory growth

---

## Next Steps

1. **Kafka Integration**: Implement Kafka producer to send batches
2. **Configuration**: Add config file support for sensor types and ranges
3. **Metrics**: Add Prometheus metrics for readings generated
4. **Logging**: Add structured logging for distributed tracing
5. **Testing**: Add unit tests for data manager operations

---

## Summary

The data-generator service demonstrates how Go's fundamental data structures (variables, structs, slices, maps) work together in a distributed microservices architecture. By using slices for batch processing and maps for quick lookups, we create an efficient foundation for the AegisAI platform that scales from development to production.
