# Data Generator - Foundation Implementation

This package implements the foundation layer for the AegisAI data-generator microservice.

## Overview

The foundation provides:
- **SensorData struct**: Core data model for sensor readings
- **DataManager**: Manages readings in memory using slices and maps
- **Random generation**: Simulates sensor data for testing

## Key Concepts

### Why Slices?
Slices are used to accumulate readings for **batch processing** before sending to Kafka:
- Better throughput (send multiple readings in one Kafka message)
- Lower network overhead
- Maintains insertion order (important for time-series data)

### Why Maps?
Maps provide **O(1) lookup** for the latest reading per sensor:
- Instant access for health checks
- Efficient threshold evaluation in alert-service
- Quick API responses for current sensor state

### Why Both?
- **Slice**: For batch operations (Kafka producer)
- **Map**: For quick lookups (health checks, alerts, APIs)

## Usage Example

```go
// Initialize manager
manager := generator.NewDataManager()

// Generate random reading
reading := generator.GenerateRandomReading(
    generator.TemperatureConfig, 
    1, // sensor number
)

// Add to manager (updates both slice and map)
manager.AddReading(reading)

// Get batch for Kafka
batch := manager.GetReadingsBatch()

// Get latest value for specific sensor
latest, exists := manager.GetLatestReading("temp-001")
```

## Integration Points

### Kafka Integration
```go
batch := manager.GetReadingsBatch()
for _, reading := range batch {
    jsonData, _ := json.Marshal(reading)
    kafkaProducer.Send("sensor-data", reading.ID, jsonData)
}
```

### DynamoDB Integration
The ingestion-service will:
1. Consume from Kafka
2. Store time-series data (partition key: SensorID, sort key: Timestamp)
3. Update latest values table (partition key: SensorID)

## Running the Example

```bash
cd services/data-generator
go run cmd/main.go
```

This demonstrates:
- Creating 5 sensor readings
- Storing in slice
- Building map of latest values
- Batch processing pattern
- O(1) lookup examples
