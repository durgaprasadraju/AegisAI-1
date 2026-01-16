# Domain Models

Pure business entities representing core concepts in the AegisAI system. These models have **zero infrastructure dependencies** and can be used across all layers (Kafka, REST APIs, ML pipelines, storage).

## Architecture

This package follows **Clean Architecture** principles:
- ✅ Depends on: Nothing (pure Go standard library)
- ❌ Does NOT depend on: Kafka, DynamoDB, AWS SDK, any frameworks

## Files

### `sensor_event.go` - SensorEvent
Represents a single IoT sensor reading event.

**Fields:**
- `EventID`: Unique identifier
- `SensorID`: Sensor identifier
- `SensorType`: Type of sensor (temperature, humidity, etc.)
- `Value`: Measured value
- `Unit`: Measurement unit
- `Timestamp`: When reading was taken
- `Tags`: Key-value metadata (map[string]string)

**Why map for Tags?**
- O(1) lookup by tag name
- No duplicates (map keys are unique)
- Natural key-value semantics
- Efficient membership checks

**Usage:**
- Kafka message serialization
- REST API request/response
- ML pipeline input
- Storage layer persistence

### `prediction.go` - Prediction
Represents ML model inference output.

**Fields:**
- `PredictionID`: Unique identifier
- `SensorID`: Sensor this prediction is for
- `ModelVersion`: ML model version
- `PredictedValue`: Predicted value
- `Confidence`: Confidence score (0.0 to 1.0)
- `GeneratedAt`: When prediction was generated
- `FeatureValues`: Input features (map[string]float64)

**Why map for FeatureValues?**
- Dynamic schema (different models have different features)
- O(1) feature lookup
- Natural key-value representation
- No code changes needed for new model versions

**Usage:**
- ML service output
- Alert evaluation
- Historical analysis

### `alert.go` - Alert
Represents an anomaly detection or threshold breach.

**Fields:**
- `AlertID`: Unique identifier
- `SensorID`: Sensor that triggered alert
- `Severity`: Alert severity (critical, warning, info)
- `Message`: Alert description
- `TriggeredAt`: When alert was triggered
- `RelatedPredictions`: Prediction IDs that contributed ([]string)

**Why slice for RelatedPredictions?**
- Preserves order (which prediction triggered first)
- Simple list semantics
- Natural iteration pattern
- Allows duplicates if needed

**Usage:**
- Alert service output
- Notification systems
- Incident management

### `metrics.go` - Metrics
Represents aggregated system performance metrics.

**Fields:**
- `ServiceName`: Service identifier
- `RequestCount`: Total requests processed
- `ErrorCount`: Total errors encountered
- `Latencies`: Ordered latency values ([]float64)
- `CollectedAt`: When metrics were collected
- `AdditionalCounters`: Optional counters (map[string]int64)
- `Labels`: Metadata tags (map[string]string)

**Why slice for Latencies?**
- Preserves order for percentile calculations (p50, p95, p99)
- Sequential processing pattern
- Time-based ordering

**Why maps for Counters/Labels?**
- O(1) lookup by metric name
- Natural key-value semantics
- Efficient aggregation

**Usage:**
- Monitoring systems
- Dashboards
- Performance analysis

## Design Principles

1. **Immutability**: Models represent point-in-time snapshots
2. **No Methods**: Pure data structures (business logic in use cases)
3. **JSON Tags**: All fields have proper JSON tags for serialization
4. **Documentation**: Each file explains business context and data structure choices

## Usage Example

```go
import "github.com/aegisai/data-generator/internal/domain"

// Create a sensor event
event := domain.SensorEvent{
    EventID:    "event-001",
    SensorID:   "sensor-001",
    SensorType: "temperature",
    Value:      25.5,
    Unit:       "celsius",
    Timestamp:  time.Now(),
    Tags: map[string]string{
        "location": "warehouse-A",
        "zone":     "cold-storage",
    },
}

// Use across layers
// - Serialize to JSON for Kafka
// - Send via REST API
// - Store in DynamoDB
// - Process in ML pipeline
```

## Data Structure Choices Explained

Each model carefully chooses between slices and maps:

- **Slices**: Used for ordered collections where order matters
  - Latencies (for percentile calculations)
  - RelatedPredictions (chronological order)

- **Maps**: Used for key-value data where lookup is important
  - Tags (O(1) lookup by tag name)
  - FeatureValues (O(1) lookup by feature name)
  - Counters/Labels (O(1) lookup by metric name)

These choices are documented in each file with explanations of why one was chosen over the other.
