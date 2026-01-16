package generator

import (
	"fmt"
	"os"
)

// ============================================================================
// CONFIGURATION - Environment-Based Configuration Loading
// ============================================================================
// This package loads configuration from environment variables with sensible
// defaults for local development. The same code works in all environments
// (dev/staging/prod) with only configuration changes.

// KafkaConfig holds Kafka broker and topic configuration.
// This configuration is used by KafkaLocalPublisher to connect to Kafka.
//
// Why environment variables?
// - Different environments need different endpoints
// - Local: localhost:9092 (Docker Kafka)
// - Staging: staging-kafka.example.com:9092
// - Production: AWS MSK broker endpoints
// - No code changes needed, only config changes
type KafkaConfig struct {
	Broker string // Kafka broker address (e.g., "localhost:9092")
	Topic  string // Kafka topic name (e.g., "sensor-data")
}

// LoadKafkaConfig loads Kafka configuration from environment variables.
// Returns a KafkaConfig with defaults suitable for local development.
//
// Environment variables:
//   - KAFKA_BROKER: Kafka broker address (default: "localhost:9092")
//   - KAFKA_TOPIC: Kafka topic name (default: "sensor-data")
//
// Example:
//   export KAFKA_BROKER=localhost:9092
//   export KAFKA_TOPIC=sensor-data
//
// In production, these would be set to:
//   export KAFKA_BROKER=msk-broker-1.example.com:9092,msk-broker-2.example.com:9092
//   export KAFKA_TOPIC=sensor-data-prod
func LoadKafkaConfig() *KafkaConfig {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092" // Default for local Docker Kafka
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "sensor-data" // Default topic name
	}

	return &KafkaConfig{
		Broker: broker,
		Topic:  topic,
	}
}

// DynamoConfig holds DynamoDB endpoint, region, and table configuration.
// This configuration is used by DynamoLocalStorage to connect to DynamoDB.
//
// Why environment variables?
// - Local: http://localhost:4566 (LocalStack)
// - Staging: DynamoDB endpoint in staging region
// - Production: Real DynamoDB endpoint (or omit endpoint for real AWS)
// - Same code, different endpoints
type DynamoConfig struct {
	Endpoint string // DynamoDB endpoint URL (empty for real AWS)
	Region   string // AWS region (e.g., "us-east-1")
	Table    string // DynamoDB table name (e.g., "sensor_readings")
}

// LoadDynamoConfig loads DynamoDB configuration from environment variables.
// Returns a DynamoConfig with defaults suitable for local development.
//
// Environment variables:
//   - DYNAMO_ENDPOINT: DynamoDB endpoint URL (default: "http://localhost:4566" for LocalStack)
//   - AWS_REGION: AWS region (default: "us-east-1")
//   - SENSOR_TABLE: DynamoDB table name (default: "sensor_readings")
//
// Example for local development:
//   export DYNAMO_ENDPOINT=http://localhost:4566
//   export AWS_REGION=us-east-1
//   export SENSOR_TABLE=sensor_readings
//
// Example for production (real AWS):
//   export DYNAMO_ENDPOINT=  # Empty or omit - uses real AWS endpoint
//   export AWS_REGION=us-east-1
//   export SENSOR_TABLE=sensor_readings_prod
//
// How LocalStack mirrors production:
// - LocalStack implements the same DynamoDB API as AWS
// - Same SDK calls work with both LocalStack and real DynamoDB
// - Only the endpoint changes, not the code
func LoadDynamoConfig() *DynamoConfig {
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566" // Default for LocalStack
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1" // Default AWS region
	}

	table := os.Getenv("SENSOR_TABLE")
	if table == "" {
		table = "sensor_readings" // Default table name
	}

	return &DynamoConfig{
		Endpoint: endpoint,
		Region:   region,
		Table:    table,
	}
}

// Validate checks if the configuration is valid.
// Returns an error if required fields are missing or invalid.
func (c *KafkaConfig) Validate() error {
	if c.Broker == "" {
		return fmt.Errorf("kafka broker address is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("kafka topic name is required")
	}
	return nil
}

// Validate checks if the configuration is valid.
// Returns an error if required fields are missing or invalid.
func (c *DynamoConfig) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("AWS region is required")
	}
	if c.Table == "" {
		return fmt.Errorf("DynamoDB table name is required")
	}
	return nil
}
