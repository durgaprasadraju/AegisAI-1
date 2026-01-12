package generator

import (
	"fmt"
	"math/rand"
	"time"
)

// ============================================================================
// SENSOR DATA STRUCTURE - Core Data Model for AegisAI
// ============================================================================

// SensorData represents a single sensor reading in the AegisAI distributed system.
// This struct is the fundamental data unit that flows through:
// 1. data-generator (producer) -> Kafka topic
// 2. ingestion-service (consumer) -> DynamoDB/S3 storage
// 3. ml-service (inference) -> gRPC responses
// 4. alert-service (threshold evaluation) -> alert triggers
//
// Why this structure?
// - ID: Unique identifier for sensor (used as partition key in DynamoDB)
// - Type: Sensor type for routing and processing logic
// - Value: The actual measurement (float64 for precision)
// - Timestamp: Critical for time-series analysis and ordering
// - Unit: Measurement unit for proper interpretation across services
type SensorData struct {
	ID        string    // Sensor identifier (e.g., "sensor-001", "temp-sensor-zone-a")
	Type      string    // Sensor type: "temperature", "humidity", "moisture", "pressure"
	Value     float64   // The actual sensor reading value
	Timestamp time.Time // When the reading was taken (critical for time-series data)
	Unit      string    // Measurement unit: "celsius", "percent", "pascal", etc.
}

// ============================================================================
// SENSOR TYPE CONSTANTS
// ============================================================================

// SensorType constants define the types of sensors in the AegisAI system.
// Using constants ensures type safety and prevents typos across microservices.
const (
	SensorTypeTemperature = "temperature"
	SensorTypeHumidity    = "humidity"
	SensorTypeMoisture    = "moisture"
	SensorTypePressure    = "pressure"
	SensorTypeLight       = "light"
)

// ============================================================================
// SENSOR UNIT CONSTANTS
// ============================================================================

// Unit constants for different sensor types.
// These ensure consistency when data flows between services.
const (
	UnitCelsius    = "celsius"
	UnitFahrenheit = "fahrenheit"
	UnitPercent    = "percent"
	UnitPascal     = "pascal"
	UnitLux        = "lux"
)

// ============================================================================
// SENSOR CONFIGURATION - Value Ranges for Random Generation
// ============================================================================

// SensorConfig defines the valid range for each sensor type.
// This is used when generating random sensor readings for testing/simulation.
type SensorConfig struct {
	Type      string
	MinValue  float64
	MaxValue  float64
	Unit      string
	IDPrefix  string // Prefix for generating sensor IDs
}

// Predefined sensor configurations for common sensor types.
var (
	TemperatureConfig = SensorConfig{
		Type:     SensorTypeTemperature,
		MinValue: -10.0,
		MaxValue: 50.0,
		Unit:     UnitCelsius,
		IDPrefix: "temp",
	}

	HumidityConfig = SensorConfig{
		Type:     SensorTypeHumidity,
		MinValue: 0.0,
		MaxValue: 100.0,
		Unit:     UnitPercent,
		IDPrefix: "humidity",
	}

	MoistureConfig = SensorConfig{
		Type:     SensorTypeMoisture,
		MinValue: 0.0,
		MaxValue: 100.0,
		Unit:     UnitPercent,
		IDPrefix: "moisture",
	}

	PressureConfig = SensorConfig{
		Type:     SensorTypePressure,
		MinValue: 980.0,
		MaxValue: 1050.0,
		Unit:     UnitPascal,
		IDPrefix: "pressure",
	}
)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// String returns a human-readable string representation of SensorData.
// Useful for logging and debugging in distributed systems.
func (s SensorData) String() string {
	return fmt.Sprintf("SensorData{ID: %s, Type: %s, Value: %.2f %s, Time: %s}",
		s.ID, s.Type, s.Value, s.Unit, s.Timestamp.Format(time.RFC3339))
}

// IsValid performs basic validation on SensorData.
// In a distributed system, validation at the producer prevents bad data
// from entering the pipeline and causing issues downstream.
func (s SensorData) IsValid() bool {
	if s.ID == "" {
		return false
	}
	if s.Type == "" {
		return false
	}
	if s.Unit == "" {
		return false
	}
	if s.Timestamp.IsZero() {
		return false
	}
	// Value can be any float64 (including negative for temperature)
	return true
}

// GenerateRandomValue generates a random value within the sensor's configured range.
// This is used by the data-generator service to simulate sensor readings.
func (config SensorConfig) GenerateRandomValue() float64 {
	// rand.Float64() returns [0.0, 1.0)
	// Scale to [MinValue, MaxValue)
	rangeSize := config.MaxValue - config.MinValue
	return config.MinValue + rand.Float64()*rangeSize
}

// GenerateSensorID creates a unique sensor ID using the prefix and a number.
// In production, sensor IDs might come from a database or configuration.
func (config SensorConfig) GenerateSensorID(number int) string {
	return fmt.Sprintf("%s-%03d", config.IDPrefix, number)
}
