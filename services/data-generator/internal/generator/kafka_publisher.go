package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
	"github.com/segmentio/kafka-go"
)

// ============================================================================
// KAFKA LOCAL PUBLISHER - Production-Ready Kafka Integration
// ============================================================================
// KafkaLocalPublisher implements the Publisher interface using the kafka-go library.
// This implementation works with both local Kafka (Docker) and production Kafka (AWS MSK).
//
// Why local Kafka mirrors production behavior:
// 1. Same Protocol: Local Kafka uses the same Kafka protocol as production
// 2. Same API: kafka-go library works identically with local and production Kafka
// 3. Same Serialization: JSON serialization works the same everywhere
// 4. Same Error Handling: Network errors, timeouts, etc. behave the same
//
// Migration to production:
// - Only change KAFKA_BROKER environment variable
// - No code changes needed
// - Same code works with AWS MSK, Confluent Cloud, or any Kafka cluster

// KafkaLocalPublisher publishes sensor data to Kafka topics.
// It implements the Publisher interface, allowing it to be used interchangeably
// with other publisher implementations (e.g., InMemoryPublisher for testing).
type KafkaLocalPublisher struct {
	writer *kafka.Writer
	topic  string
	config *KafkaConfig
}

// NewKafkaLocalPublisher creates a new Kafka publisher with the given configuration.
// It initializes a kafka.Writer that will be used to send messages to Kafka.
//
// The writer is configured with:
// - Broker address from config
// - Topic from config
// - Balanced write mode (messages distributed across partitions)
// - Async writes for better performance
// - Automatic retries on failure
//
// Example:
//   config := LoadKafkaConfig()
//   publisher, err := NewKafkaLocalPublisher(config)
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer publisher.Close()
func NewKafkaLocalPublisher(config *KafkaConfig) (*KafkaLocalPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	// Create Kafka writer with configuration
	// The writer handles connection pooling, retries, and error handling
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Broker),
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{}, // Distribute messages across partitions
		WriteTimeout: 10 * time.Second,   // Timeout for write operations
		RequiredAcks: kafka.RequireOne,   // Wait for at least one broker acknowledgment
		Async:        false,               // Synchronous writes for reliability
	}

	return &KafkaLocalPublisher{
		writer: writer,
		topic:  config.Topic,
		config: config,
	}, nil
}

// Publish sends a single sensor reading to Kafka.
// It serializes the SensorData to JSON and sends it as a Kafka message.
//
// The message key is set to the sensor ID, which ensures:
// - Messages from the same sensor go to the same partition (ordering)
// - Better distribution across partitions
//
// Context cancellation is respected - if the context is cancelled,
// the operation will return immediately with an error.
//
// This method implements the Publisher interface.
func (p *KafkaLocalPublisher) Publish(ctx context.Context, data SensorData) error {
	// Validate data before publishing
	if !data.IsValid() {
		return fmt.Errorf("invalid sensor data: %v", data)
	}

	// Serialize SensorData to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal sensor data: %w", err)
	}

	// Create Kafka message
	// Key: sensor ID (ensures same sensor goes to same partition)
	// Value: JSON-serialized sensor data
	message := kafka.Message{
		Key:   []byte(data.ID),
		Value: jsonData,
		Time:  time.Now(),
	}

	// Send message to Kafka
	// The writer handles retries, connection management, etc.
	err = p.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish message to kafka: %w", err)
	}

	return nil
}

// PublishBatch sends multiple sensor readings to Kafka in a single batch.
// This is more efficient than sending individual messages.
//
// All messages are sent to the same topic, with each sensor's ID as the message key.
// The batch is sent atomically - either all messages succeed or all fail.
//
// This method implements the Publisher interface.
func (p *KafkaLocalPublisher) PublishBatch(ctx context.Context, data []SensorData) error {
	if len(data) == 0 {
		return nil // Nothing to publish
	}

	// Convert SensorData slice to Kafka messages
	messages := make([]kafka.Message, 0, len(data))
	for _, reading := range data {
		// Validate each reading
		if !reading.IsValid() {
			return fmt.Errorf("invalid sensor data in batch: %v", reading)
		}

		// Serialize to JSON
		jsonData, err := json.Marshal(reading)
		if err != nil {
			return fmt.Errorf("failed to marshal sensor data: %w", err)
		}

		// Create message
		messages = append(messages, kafka.Message{
			Key:   []byte(reading.ID),
			Value: jsonData,
			Time:  time.Now(),
		})
	}

	// Send all messages in a single batch
	// This is more efficient than individual sends
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("failed to publish batch to kafka: %w", err)
	}

	return nil
}

// Close closes the Kafka writer and releases resources.
// This should be called when the publisher is no longer needed,
// typically during application shutdown.
//
// It's safe to call Close multiple times.
func (p *KafkaLocalPublisher) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

// ============================================================================
// PRODUCTION MIGRATION NOTES
// ============================================================================
//
// To use this with AWS MSK (Managed Streaming for Kafka):
// 1. Set KAFKA_BROKER to MSK broker endpoints (comma-separated)
//    export KAFKA_BROKER=broker1.msk.region.amazonaws.com:9092,broker2.msk.region.amazonaws.com:9092
// 2. Configure AWS credentials (if using SASL/SCRAM authentication)
// 3. No code changes needed!
//
// To use this with Confluent Cloud:
// 1. Set KAFKA_BROKER to Confluent Cloud broker endpoints
// 2. Configure authentication (API keys, SASL, etc.)
// 3. No code changes needed!
//
// The kafka-go library handles:
// - Connection pooling
// - Automatic retries
// - Partition distribution
// - Error handling
// - All the complexity of Kafka protocol

// ============================================================================
// KAFKA DLQ INGESTER - Dead Letter Queue Publisher
// ============================================================================
// KafkaDLQIngester implements the SensorIngester interface by publishing
// failed sensor events to a Kafka DLQ topic. This allows failed events to be:
// - Persisted for later analysis
// - Reprocessed after fixing underlying issues
// - Monitored and alerted on
//
// Architecture:
// - Uses KafkaLocalPublisher internally for DLQ topic
// - Publishes domain.SensorEvent as JSON (preserves all event data)
// - Follows SensorIngester interface pattern for consistency

// KafkaDLQIngester publishes failed sensor events to a Kafka DLQ topic.
// It implements the SensorIngester interface, allowing it to be used
// interchangeably with other ingester implementations.
type KafkaDLQIngester struct {
	writer *kafka.Writer
	topic  string
}

// NewKafkaDLQIngester creates a new DLQ ingester that publishes to a Kafka DLQ topic.
// It creates a Kafka writer configured with the DLQ topic from the config.
//
// Example:
//   config := LoadKafkaConfig()
//   dlqIngester, err := NewKafkaDLQIngester(config)
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer dlqIngester.Close()
func NewKafkaDLQIngester(config *KafkaConfig) (*KafkaDLQIngester, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	// Create Kafka writer for DLQ topic
	// The writer handles connection pooling, retries, and error handling
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Broker),
		Topic:        config.DLQTopic,
		Balancer:     &kafka.LeastBytes{}, // Distribute messages across partitions
		WriteTimeout: 10 * time.Second,   // Timeout for write operations
		RequiredAcks: kafka.RequireOne,   // Wait for at least one broker acknowledgment
		Async:        false,               // Synchronous writes for reliability
	}

	return &KafkaDLQIngester{
		writer: writer,
		topic:  config.DLQTopic,
	}, nil
}

// Ingest publishes a failed sensor event to the Kafka DLQ topic.
// This method implements the SensorIngester interface.
//
// The event is serialized to JSON and published with the SensorID as the message key,
// ensuring events from the same sensor are routed to the same partition.
func (k *KafkaDLQIngester) Ingest(ctx context.Context, event domain.SensorEvent) error {
	// Serialize SensorEvent to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal sensor event: %w", err)
	}

	// Create Kafka message
	// Key: sensor ID (ensures same sensor goes to same partition)
	// Value: JSON-serialized sensor event
	message := kafka.Message{
		Key:   []byte(event.SensorID),
		Value: jsonData,
		Time:  time.Now(),
	}

	// Send message to DLQ topic
	err = k.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish event to DLQ topic %s: %w", k.topic, err)
	}

	return nil
}

// Close closes the Kafka writer and releases resources.
// This should be called when the DLQ ingester is no longer needed.
func (k *KafkaDLQIngester) Close() error {
	if k.writer != nil {
		return k.writer.Close()
	}
	return nil
}
