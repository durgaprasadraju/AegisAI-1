package domain

import "time"

// Prediction represents an ML inference output for a sensor.
// This is a pure domain model - no infrastructure dependencies.
//
// Business Context:
// - Result of ML model inference on sensor data
// - Used by ML service to communicate predictions to other services
// - Can be stored, queried, and used for alerting decisions
// - Immutable by design: represents a prediction at a point in time
//
// Data Structure Choice: FeatureValues as map[string]float64
// Why map instead of struct or slice?
//
// 1. Dynamic schema: ML models may have different feature sets
//    - Model v1.0: temperature, humidity, pressure
//    - Model v2.0: temperature, humidity, pressure, wind_speed, cloud_cover
//    - Struct would require code changes for each model version
//    - Map allows runtime flexibility without recompilation
//
// 2. Fast feature lookup: O(1) access by feature name
//    - prediction.FeatureValues["temperature"] vs linear search in slice
//
// 3. Natural representation: Features are named values
//    - temperature=25.3, humidity=65.2, pressure=1013.25
//    - Map directly represents this key-value relationship
//
// Alternative (struct) would require:
// - Knowing all features at compile time
// - Code changes for each model version
// - Cannot handle dynamic feature sets
//
// Alternative (slice) would require:
// - Maintaining order and searching linearly
// - More complex: type Feature struct { Name string; Value float64 }
// - O(n) lookup instead of O(1)
type Prediction struct {
	// PredictionID uniquely identifies this prediction
	PredictionID string `json:"prediction_id"`

	// SensorID identifies the sensor this prediction is for
	SensorID string `json:"sensor_id"`

	// ModelVersion identifies which ML model generated this prediction
	// Examples: "v1.0", "v2.1", "anomaly-detector-v3"
	ModelVersion string `json:"model_version"`

	// PredictedValue is the ML model's predicted value
	PredictedValue float64 `json:"predicted_value"`

	// Confidence is the model's confidence in this prediction (0.0 to 1.0)
	Confidence float64 `json:"confidence"`

	// GeneratedAt is when the prediction was generated
	GeneratedAt time.Time `json:"generated_at"`

	// FeatureValues are the input features used to generate this prediction
	// Examples: {"temperature": 25.3, "humidity": 65.2, "pressure": 1013.25}
	// Using map[string]float64 for dynamic schema and O(1) feature lookup
	FeatureValues map[string]float64 `json:"feature_values,omitempty"`
}
