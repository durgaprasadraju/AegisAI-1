package generator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// DATA MANAGER - Manages Sensor Data in Memory
// ============================================================================

// DataManager handles in-memory storage of sensor readings.
// In a distributed system, this serves as a buffer before sending to Kafka.
//
// Why use both slices AND maps?
// - Slices: Efficient for batch operations (sending multiple readings to Kafka)
// - Maps: O(1) lookup for latest value per sensor (useful for health checks, alerts)
type DataManager struct {
	// readings is a slice that accumulates sensor readings.
	// Slices are used here because:
	// 1. We need to batch readings before sending to Kafka (better throughput)
	// 2. Slices maintain insertion order (important for time-series data)
	// 3. Easy to iterate and process in batches
	// 4. Memory-efficient for sequential data
	readings []SensorData

	// latestReadings is a map that stores the most recent reading per sensor ID.
	// Maps are used here because:
	// 1. O(1) lookup time - instant access to latest value for any sensor
	// 2. Perfect for health checks and alert evaluation
	// 3. Automatically handles updates (new reading replaces old for same ID)
	// 4. Can be quickly serialized and sent to monitoring systems
	latestReadings map[string]SensorData

	// mutex protects concurrent access to shared data structures.
	// In a distributed system, the data-generator might have multiple goroutines
	// generating readings concurrently, so we need thread-safe access.
	mu sync.RWMutex
}

// NewDataManager creates a new DataManager instance.
// This initializes the internal data structures.
func NewDataManager() *DataManager {
	return &DataManager{
		readings:       make([]SensorData, 0, 100), // Pre-allocate capacity for efficiency
		latestReadings: make(map[string]SensorData),
	}
}

// ============================================================================
// SLICE OPERATIONS - For Batch Processing
// ============================================================================

// AddReading adds a new sensor reading to the slice.
// This is the primary method for accumulating readings before batch sending to Kafka.
//
// Why add to slice?
// - Kafka producers work best with batches (better throughput, lower overhead)
// - We can accumulate readings and send them together
// - Reduces network round-trips in distributed systems
func (dm *DataManager) AddReading(reading SensorData) error {
	// Validate before adding (fail fast principle)
	if !reading.IsValid() {
		return fmt.Errorf("invalid sensor reading: %v", reading)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Add to slice (append is O(1) amortized)
	dm.readings = append(dm.readings, reading)

	// Also update the latest readings map
	// This keeps both data structures in sync
	dm.latestReadings[reading.ID] = reading

	return nil
}

// GetReadingsBatch returns a batch of readings and clears the internal slice.
// This is called before sending data to Kafka.
//
// Why return and clear?
// - After sending to Kafka, we don't need to keep readings in memory
// - Prevents memory growth in long-running services
// - The map still has latest values for quick lookups
func (dm *DataManager) GetReadingsBatch() []SensorData {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Create a copy of the slice to return
	batch := make([]SensorData, len(dm.readings))
	copy(batch, dm.readings)

	// Clear the slice (but keep capacity for efficiency)
	dm.readings = dm.readings[:0]

	return batch
}

// GetReadingsCount returns the number of readings currently in the slice.
// Useful for monitoring and deciding when to flush to Kafka.
func (dm *DataManager) GetReadingsCount() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.readings)
}

// ============================================================================
// MAP OPERATIONS - For Latest Value Lookups
// ============================================================================

// UpdateLatestReading updates the map with the latest reading for a sensor.
// This is automatically called by AddReading, but can be called explicitly
// if you only want to update the map without adding to the batch slice.
func (dm *DataManager) UpdateLatestReading(reading SensorData) error {
	if !reading.IsValid() {
		return fmt.Errorf("invalid sensor reading: %v", reading)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if this reading is newer than the existing one
	existing, exists := dm.latestReadings[reading.ID]
	if !exists || reading.Timestamp.After(existing.Timestamp) {
		dm.latestReadings[reading.ID] = reading
	}

	return nil
}

// GetLatestReading returns the most recent reading for a sensor ID.
// O(1) lookup time - perfect for health checks and alert evaluation.
func (dm *DataManager) GetLatestReading(sensorID string) (SensorData, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	reading, exists := dm.latestReadings[sensorID]
	return reading, exists
}

// GetAllLatestReadings returns all latest readings as a map.
// Useful for exporting current state to monitoring systems or API endpoints.
func (dm *DataManager) GetAllLatestReadings() map[string]SensorData {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// Create a copy to prevent external modifications
	result := make(map[string]SensorData, len(dm.latestReadings))
	for k, v := range dm.latestReadings {
		result[k] = v
	}

	return result
}

// GetLatestReadingsCount returns the number of unique sensors.
func (dm *DataManager) GetLatestReadingsCount() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.latestReadings)
}

// ============================================================================
// RANDOM GENERATION - For Simulating Sensor Data
// ============================================================================

// GenerateRandomReading creates a random sensor reading based on the provided config.
// This simulates real sensor behavior for the data-generator service.
//
// In production, this would be replaced by actual sensor hardware interfaces.
func GenerateRandomReading(config SensorConfig, sensorNumber int) SensorData {
	return SensorData{
		ID:        config.GenerateSensorID(sensorNumber),
		Type:      config.Type,
		Value:     config.GenerateRandomValue(),
		Timestamp: time.Now(),
		Unit:      config.Unit,
	}
}

// GenerateRandomReadings creates multiple random readings for different sensors.
// Useful for testing and simulation scenarios.
func GenerateRandomReadings(config SensorConfig, count int) []SensorData {
	readings := make([]SensorData, 0, count)
	for i := 1; i <= count; i++ {
		readings = append(readings, GenerateRandomReading(config, i))
	}
	return readings
}

// ============================================================================
// CONTEXT-AWARE OPERATIONS - For Distributed Systems
// ============================================================================

// AddReadingWithContext adds a reading with context support.
// Context allows cancellation and timeout handling, which is crucial in
// distributed systems where operations might need to be cancelled.
func (dm *DataManager) AddReadingWithContext(ctx context.Context, reading SensorData) error {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Add the reading (this is fast, so we don't need to check context again)
	return dm.AddReading(reading)
}

// FlushReadingsWithContext flushes readings to a callback function with context support.
// This pattern will be used when sending to Kafka - the callback will be the Kafka producer.
func (dm *DataManager) FlushReadingsWithContext(ctx context.Context, flushFunc func([]SensorData) error) error {
	// Get the batch
	batch := dm.GetReadingsBatch()

	if len(batch) == 0 {
		return nil // Nothing to flush
	}

	// Check context before flushing
	select {
	case <-ctx.Done():
		// Context cancelled - put readings back (or handle as needed)
		return ctx.Err()
	default:
	}

	// Call the flush function (e.g., send to Kafka)
	return flushFunc(batch)
}

// ============================================================================
// INTEGRATION NOTES FOR DISTRIBUTED SYSTEMS
// ============================================================================

/*
KAFKA INTEGRATION PATTERN:
-------------------------
1. Readings accumulate in the slice (via AddReading)
2. Periodically (or when batch size reached), call GetReadingsBatch()
3. Serialize batch to JSON/Protobuf
4. Send to Kafka topic (e.g., "sensor-data")
5. Kafka partitions by sensor ID for ordering guarantees

Example:
  batch := manager.GetReadingsBatch()
  for _, reading := range batch {
    jsonData, _ := json.Marshal(reading)
    kafkaProducer.Send("sensor-data", reading.ID, jsonData)
  }


DYNAMODB INTEGRATION PATTERN:
------------------------------
1. ingestion-service consumes from Kafka
2. For each reading, store in DynamoDB:
   - Partition key: SensorID
   - Sort key: Timestamp (for time-series queries)
3. Also update latest reading in a separate table:
   - Partition key: SensorID
   - Single item per sensor (always latest value)

Example:
  // Store time-series data
  dynamodb.PutItem("sensor-readings", {
    "SensorID": reading.ID,
    "Timestamp": reading.Timestamp,
    "Value": reading.Value,
    ...
  })
  
  // Update latest value
  dynamodb.PutItem("sensor-latest", {
    "SensorID": reading.ID,
    ...reading
  })


MONITORING INTEGRATION:
-----------------------
- Use GetAllLatestReadings() to expose current state via REST API
- Alert-service can query latest values for threshold evaluation
- Prometheus metrics can be derived from latest readings map
*/
