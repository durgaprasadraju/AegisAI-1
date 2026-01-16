package domain

import "time"

// Alert represents an anomaly detection or threshold breach event.
// This is a pure domain model - no infrastructure dependencies.
//
// Business Context:
// - Triggered when sensor values exceed thresholds or anomalies are detected
// - Can be related to one or more ML predictions that contributed to the alert
// - Used for notification systems, dashboards, and incident management
// - Immutable by design: represents an alert at a point in time
//
// Data Structure Choice: RelatedPredictions as []string (slice)
// Why slice instead of map?
//
// 1. Ordered sequence: Order matters - which prediction triggered first?
//    - First prediction may have detected the anomaly
//    - Subsequent predictions may have confirmed it
//    - Slice preserves chronological order
//
// 2. Simple list semantics: Just a list of prediction IDs
//    - No key-value relationship needed
//    - No need for O(1) lookup by key (we iterate through them)
//    - Natural representation: "these predictions are related to this alert"
//
// 3. Iteration pattern: We typically iterate through all related predictions
//    - for _, predID := range alert.RelatedPredictions { ... }
//    - Map would require: for predID := range alert.RelatedPredictions { ... }
//    - Slice iteration is more natural for ordered lists
//
// 4. Duplicates allowed: Same prediction might be referenced multiple times
//    - Though rare, slice allows this if needed
//    - Map keys would prevent duplicates (which may or may not be desired)
//
// Alternative (map[string]bool) would:
// - Lose ordering information
// - Add unnecessary complexity for simple ID list
// - Require map[string]bool instead of []string
type Alert struct {
	// AlertID uniquely identifies this alert
	AlertID string `json:"alert_id"`

	// SensorID identifies the sensor that triggered this alert
	SensorID string `json:"sensor_id"`

	// Severity indicates the alert severity level
	// Examples: "critical", "warning", "info"
	Severity string `json:"severity"`

	// Message describes what triggered this alert
	// Examples: "Temperature exceeded threshold: 30°C > 25°C"
	Message string `json:"message"`

	// TriggeredAt is when the alert was triggered
	TriggeredAt time.Time `json:"triggered_at"`

	// RelatedPredictions are prediction IDs that contributed to this alert
	// Using slice to preserve order and represent simple list semantics
	// Example: ["pred-001", "pred-002"] - pred-001 detected, pred-002 confirmed
	RelatedPredictions []string `json:"related_predictions,omitempty"`
}
