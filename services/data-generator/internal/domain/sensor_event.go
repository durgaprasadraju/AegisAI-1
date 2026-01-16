package domain

import "time"

// SensorEvent represents a single IoT sensor reading event.
// This is a pure domain model - no infrastructure dependencies.
//
// Business Context:
// - Captures a single measurement from an IoT sensor at a point in time
// - Used across the system: Kafka messages, REST APIs, ML pipelines, storage
// - Immutable by design: represents a point-in-time observation
//
// Data Structure Choice: Tags as map[string]string
// Why map instead of slice?
// 1. Fast lookup: O(1) access by tag name (e.g., tags["location"])
// 2. No duplicates: map keys are unique, prevents duplicate tag names
// 3. Natural semantics: tags are key-value metadata (location="warehouse-A", zone="cold-storage")
// 4. Efficient membership checks: O(1) to check if tag exists
//
// Alternative (slice) would require:
// - O(n) linear search to find a tag
// - Manual duplicate prevention
// - More complex code: for _, tag := range tags { if tag.Key == "location" { ... } }
type SensorEvent struct {
	// EventID uniquely identifies this event across the system
	EventID string `json:"event_id"`

	// SensorID identifies the sensor that generated this reading
	SensorID string `json:"sensor_id"`

	// SensorType categorizes the sensor (temperature, humidity, pressure, etc.)
	SensorType string `json:"sensor_type"`

	// Value is the measured value from the sensor
	Value float64 `json:"value"`

	// Unit is the measurement unit (Celsius, Fahrenheit, %, Pa, etc.)
	Unit string `json:"unit"`

	// Timestamp is when the sensor reading was taken
	Timestamp time.Time `json:"timestamp"`

	// Tags are key-value metadata associated with this event
	// Examples: location="warehouse-A", zone="cold-storage", device="sensor-001"
	// Using map[string]string for O(1) lookup and natural key-value semantics
	Tags map[string]string `json:"tags,omitempty"`
}
