package generator

import (
	"fmt"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
)

// ============================================================================
// PURE FUNCTIONS - Stateless Data Manipulation
// ============================================================================
// These functions are pure (no side effects) and can be easily tested.
// They operate on data structures without modifying shared state.
// This makes the code more predictable and easier to reason about.

// GenerateSensorData creates a new SensorData instance for the given sensor type.
// This is a pure function that generates sensor data based on configuration.
//
// Why a function instead of a method?
// - Functions are stateless and easier to test
// - Can be used with different configurations
// - Follows functional programming principles
func GenerateSensorData(sensorType string, sensorNumber int) SensorData {
	// Get the appropriate config based on sensor type
	var config SensorConfig
	switch sensorType {
	case SensorTypeTemperature:
		config = TemperatureConfig
	case SensorTypeHumidity:
		config = HumidityConfig
	case SensorTypeMoisture:
		config = MoistureConfig
	case SensorTypePressure:
		config = PressureConfig
	default:
		// Default to temperature if unknown type
		config = TemperatureConfig
	}

	return SensorData{
		ID:        config.GenerateSensorID(sensorNumber),
		Type:      config.Type,
		Value:     config.GenerateRandomValue(),
		Timestamp: time.Now(),
		Unit:      config.Unit,
	}
}

// AddReading is a pure function that adds a SensorData to a slice.
// It returns a new slice with the reading appended, following immutability principles.
//
// Why return a new slice instead of modifying in place?
// - Immutability makes code safer in concurrent scenarios
// - Easier to reason about (no hidden mutations)
// - Can be used in functional programming patterns
//
// Note: In performance-critical paths, you might still use append in place,
// but this function demonstrates the pure function pattern.
func AddReading(readings []SensorData, data SensorData) []SensorData {
	// Validate the reading before adding
	if !data.IsValid() {
		return readings // Return unchanged if invalid
	}

	// Append and return new slice
	// Go's append may reuse underlying array, but from caller's perspective it's a new slice
	return append(readings, data)
}

// UpdateLatestReadings is a pure function that updates a map with the latest reading.
// It returns a new map with the updated value, maintaining immutability.
//
// Why return a new map?
// - Prevents accidental mutations of shared maps
// - Makes concurrent access safer
// - Easier to test and reason about
//
// Note: For performance, you might modify the map in place in production,
// but this demonstrates the functional approach.
func UpdateLatestReadings(latest map[string]SensorData, data SensorData) map[string]SensorData {
	// Validate the reading
	if !data.IsValid() {
		return latest // Return unchanged if invalid
	}

	// Create a new map (copy existing entries)
	result := make(map[string]SensorData, len(latest)+1)
	for k, v := range latest {
		result[k] = v
	}

	// Check if this reading is newer than existing one
	existing, exists := result[data.ID]
	if !exists || data.Timestamp.After(existing.Timestamp) {
		result[data.ID] = data
	}

	return result
}

// FilterReadingsByType filters a slice of readings by sensor type.
// This is a pure function that returns a new slice containing only matching readings.
func FilterReadingsByType(readings []SensorData, sensorType string) []SensorData {
	result := make([]SensorData, 0)
	for _, reading := range readings {
		if reading.Type == sensorType {
			result = append(result, reading)
		}
	}
	return result
}

// GetLatestReadingFromMap retrieves the latest reading for a sensor ID from a map.
// This is a pure function that doesn't modify the input map.
func GetLatestReadingFromMap(latest map[string]SensorData, sensorID string) (SensorData, bool) {
	reading, exists := latest[sensorID]
	return reading, exists
}

// ============================================================================
// SENSOR EVENT GENERATION - Domain Model
// ============================================================================

// GenerateSensorEvent creates a new SensorEvent instance for the given sensor type.
// This generates domain.SensorEvent (the newer domain model) instead of SensorData.
// It includes EventID, Tags, and other metadata that SensorData doesn't have.
//
// Why use domain.SensorEvent instead of SensorData?
// - SensorEvent is the domain model used throughout use cases
// - Includes EventID for unique identification
// - Includes Tags for metadata (location, zone, etc.)
// - Consistent with Clean Architecture (domain layer)
//
// This function is a pure function - no side effects, easy to test.
func GenerateSensorEvent(sensorType string, sensorNumber int, eventIndex int) domain.SensorEvent {
	// Get the appropriate config based on sensor type
	var config SensorConfig
	switch sensorType {
	case SensorTypeTemperature:
		config = TemperatureConfig
	case SensorTypeHumidity:
		config = HumidityConfig
	case SensorTypeMoisture:
		config = MoistureConfig
	case SensorTypePressure:
		config = PressureConfig
	default:
		// Default to temperature if unknown type
		config = TemperatureConfig
	}

	// Generate sensor ID
	sensorID := config.GenerateSensorID(sensorNumber)

	// Generate event ID
	eventID := fmt.Sprintf("event-%03d", eventIndex)

	// Generate timestamp
	timestamp := time.Now().Add(time.Duration(eventIndex) * time.Second)

	// Create SensorEvent with Tags
	return domain.SensorEvent{
		EventID:    eventID,
		SensorID:   sensorID,
		SensorType: config.Type,
		Value:      config.GenerateRandomValue(),
		Unit:       config.Unit,
		Timestamp:  timestamp,
		Tags: map[string]string{
			"location":   "warehouse-A",
			"zone":       fmt.Sprintf("zone-%d", (eventIndex%3)+1),
			"source":     "data-generator",
			"sensor_type": config.Type,
		},
	}
}

// GenerateSensorEvents creates multiple SensorEvent instances.
// This is useful for batch generation in examples and tests.
func GenerateSensorEvents(sensorType string, sensorNumber int, count int) []domain.SensorEvent {
	events := make([]domain.SensorEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, GenerateSensorEvent(sensorType, sensorNumber, i))
	}
	return events
}
