package generator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// CONCRETE IMPLEMENTATIONS - In-Memory for Development/Testing
// ============================================================================
// These are concrete implementations of the interfaces.
// In production, these will be replaced with:
// - KafkaPublisher (for Publisher interface)
// - DynamoDBStorage (for Storage interface)
// - RealSensorGenerator (for SensorGenerator interface)

// RandomSensorGenerator implements SensorGenerator by generating random sensor readings.
// This is used for simulation and testing.
// In production, this would be replaced with a real hardware sensor interface.
type RandomSensorGenerator struct {
	sensorType   string
	sensorNumber int
	config       SensorConfig
}

// NewRandomSensorGenerator creates a new random sensor generator.
func NewRandomSensorGenerator(sensorType string, sensorNumber int) *RandomSensorGenerator {
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
		config = TemperatureConfig
	}

	return &RandomSensorGenerator{
		sensorType:   sensorType,
		sensorNumber: sensorNumber,
		config:       config,
	}
}

// Generate creates a random sensor reading.
// This satisfies the SensorGenerator interface.
func (g *RandomSensorGenerator) Generate() SensorData {
	return SensorData{
		ID:        g.config.GenerateSensorID(g.sensorNumber),
		Type:      g.config.Type,
		Value:     g.config.GenerateRandomValue(),
		Timestamp: time.Now(),
		Unit:      g.config.Unit,
	}
}

// InMemoryPublisher implements Publisher by logging data to console.
// This is a placeholder implementation for development and testing.
// In production, this will be replaced with KafkaPublisher.
type InMemoryPublisher struct {
	publishedData []SensorData // Store published data for inspection
	mu            sync.RWMutex
}

// NewInMemoryPublisher creates a new in-memory publisher.
func NewInMemoryPublisher() *InMemoryPublisher {
	return &InMemoryPublisher{
		publishedData: make([]SensorData, 0),
	}
}

// Publish logs the sensor data (simulating publishing to Kafka).
// In production, this would serialize data and send to Kafka topic.
func (p *InMemoryPublisher) Publish(ctx context.Context, data SensorData) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Validate data
	if !data.IsValid() {
		return fmt.Errorf("invalid sensor data: %v", data)
	}

	// In production: kafkaProducer.Send("sensor-data", data.ID, jsonData)
	// For now, just log and store
	p.mu.Lock()
	p.publishedData = append(p.publishedData, data)
	p.mu.Unlock()

	fmt.Printf("[PUBLISHER] Published: %s\n", data.String())
	return nil
}

// PublishBatch publishes multiple readings in a batch.
// In production, this would send all readings in a single Kafka message batch.
func (p *InMemoryPublisher) PublishBatch(ctx context.Context, data []SensorData) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// In production: kafkaProducer.SendBatch("sensor-data", batch)
	// For now, publish each individually (in real Kafka, this would be one batch)
	for _, reading := range data {
		if err := p.Publish(ctx, reading); err != nil {
			return fmt.Errorf("failed to publish reading %s: %w", reading.ID, err)
		}
	}

	fmt.Printf("[PUBLISHER] Published batch of %d readings\n", len(data))
	return nil
}

// GetPublishedData returns all published data (for testing/inspection).
func (p *InMemoryPublisher) GetPublishedData() []SensorData {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]SensorData, len(p.publishedData))
	copy(result, p.publishedData)
	return result
}

// InMemoryStorage implements Storage by storing data in memory (slice and map).
// This is a placeholder implementation for development and testing.
// In production, this will be replaced with DynamoDBStorage.
type InMemoryStorage struct {
	readings       []SensorData
	latestReadings map[string]SensorData
	mu             sync.RWMutex
}

// NewInMemoryStorage creates a new in-memory storage.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		readings:       make([]SensorData, 0),
		latestReadings: make(map[string]SensorData),
	}
}

// Save stores a sensor reading in memory.
// In production, this would save to DynamoDB time-series table.
func (s *InMemoryStorage) Save(ctx context.Context, data SensorData) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Validate data
	if !data.IsValid() {
		return fmt.Errorf("invalid sensor data: %v", data)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Store in slice (for time-series)
	s.readings = append(s.readings, data)

	// Update latest reading map
	existing, exists := s.latestReadings[data.ID]
	if !exists || data.Timestamp.After(existing.Timestamp) {
		s.latestReadings[data.ID] = data
	}

	// In production: dynamodb.PutItem("sensor-readings", {...})
	fmt.Printf("[STORAGE] Saved: %s\n", data.String())
	return nil
}

// SaveBatch stores multiple readings in a batch operation.
// In production, this would use DynamoDB batch write for efficiency.
func (s *InMemoryStorage) SaveBatch(ctx context.Context, data []SensorData) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// In production: dynamodb.BatchWriteItem(items)
	// For now, save each individually
	for _, reading := range data {
		if err := s.Save(ctx, reading); err != nil {
			return fmt.Errorf("failed to save reading %s: %w", reading.ID, err)
		}
	}

	fmt.Printf("[STORAGE] Saved batch of %d readings\n", len(data))
	return nil
}

// GetLatest retrieves the most recent reading for a sensor.
// In production, this would query DynamoDB latest values table.
func (s *InMemoryStorage) GetLatest(ctx context.Context, sensorID string) (SensorData, bool, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return SensorData{}, false, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// In production: dynamodb.GetItem("sensor-latest", {"SensorID": sensorID})
	reading, exists := s.latestReadings[sensorID]
	return reading, exists, nil
}

// GetAllReadings returns all stored readings (for testing/inspection).
func (s *InMemoryStorage) GetAllReadings() []SensorData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]SensorData, len(s.readings))
	copy(result, s.readings)
	return result
}

// GetLatestReadings returns all latest readings (for testing/inspection).
func (s *InMemoryStorage) GetLatestReadings() map[string]SensorData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]SensorData)
	for k, v := range s.latestReadings {
		result[k] = v
	}
	return result
}

// ============================================================================
// FUTURE IMPLEMENTATIONS (Placeholders for Documentation)
// ============================================================================
//
// These will be implemented in future iterations:
//
// type KafkaPublisher struct {
//     producer *kafka.Producer
//     topic    string
// }
//
// func (p *KafkaPublisher) Publish(ctx context.Context, data SensorData) error {
//     jsonData, _ := json.Marshal(data)
//     return p.producer.Send(ctx, &kafka.Message{
//         Topic: p.topic,
//         Key:   []byte(data.ID),
//         Value: jsonData,
//     })
// }
//
// type DynamoDBStorage struct {
//     client *dynamodb.DynamoDB
//     table  string
// }
//
// func (s *DynamoDBStorage) Save(ctx context.Context, data SensorData) error {
//     item := map[string]interface{}{
//         "SensorID":  data.ID,
//         "Timestamp": data.Timestamp.Unix(),
//         "Type":      data.Type,
//         "Value":     data.Value,
//         "Unit":      data.Unit,
//     }
//     return s.client.PutItem(ctx, s.table, item)
// }
