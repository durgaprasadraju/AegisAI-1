package usecase

import (
	"fmt"
	"os"
	"strconv"
)

// ============================================================================
// CONFIGURATION - Environment-Based Settings
// ============================================================================
// PipelineConfig holds configuration for the ingestion pipeline.
// Values are loaded from environment variables with sensible defaults.
//
// Why environment variables?
// - Different environments need different settings
// - Local: smaller worker pool, smaller buffer
// - Production: larger worker pool, larger buffer
// - No code changes needed, only config changes

// PipelineConfig holds configuration for the concurrent ingestion pipeline
type PipelineConfig struct {
	// WorkerCount is the number of concurrent workers in the pool
	// More workers = higher throughput, but more resource usage
	WorkerCount int

	// ChannelBufferSize is the size of the buffered input channel
	// Larger buffer = less blocking, but more memory usage
	// Should be sized based on expected event rate
	ChannelBufferSize int
}

// ConsumerGroupConfig holds configuration for the Kafka-like consumer group simulation
type ConsumerGroupConfig struct {
	// PartitionCount is the number of partitions (channels) to simulate
	// More partitions = more parallelism, but more overhead
	// In Kafka, this matches the topic partition count
	PartitionCount int

	// WorkersPerPartition is the number of worker goroutines per partition
	// More workers = higher throughput per partition
	// Total workers = PartitionCount * WorkersPerPartition
	WorkersPerPartition int

	// MaxRetries is the maximum number of retry attempts before sending to DLQ
	// 0 = no retries, event goes directly to DLQ on failure
	MaxRetries int

	// DLQBufferSize is the size of the Dead Letter Queue channel buffer
	// Failed events after max retries are sent here
	DLQBufferSize int

	// BackpressureMode controls behavior when partition channel is full
	// "blocking" = producer blocks until space available (default, safer)
	// "dropping" = event is dropped with warning (higher throughput, data loss risk)
	BackpressureMode string
}

// LoadPipelineConfig loads pipeline configuration from environment variables.
// Returns a PipelineConfig with defaults suitable for local development.
//
// Environment variables:
//   - WORKER_COUNT: Number of worker goroutines (default: 5)
//   - CHANNEL_BUFFER_SIZE: Size of input channel buffer (default: 100)
//
// Example:
//   export WORKER_COUNT=10
//   export CHANNEL_BUFFER_SIZE=200
func LoadPipelineConfig() *PipelineConfig {
	workerCount := getEnvInt("WORKER_COUNT", 5)
	bufferSize := getEnvInt("CHANNEL_BUFFER_SIZE", 100)

	// Validate configuration
	if workerCount < 1 {
		fmt.Printf("Warning: WORKER_COUNT must be >= 1, using default: 5\n")
		workerCount = 5
	}
	if bufferSize < 1 {
		fmt.Printf("Warning: CHANNEL_BUFFER_SIZE must be >= 1, using default: 100\n")
		bufferSize = 100
	}

	return &PipelineConfig{
		WorkerCount:       workerCount,
		ChannelBufferSize: bufferSize,
	}
}

// LoadConsumerGroupConfig loads consumer group configuration from environment variables.
// Returns a ConsumerGroupConfig with defaults suitable for local development.
//
// Environment variables:
//   - PARTITION_COUNT: Number of partitions (default: 3)
//   - WORKERS_PER_PARTITION: Workers per partition (default: 2)
//   - MAX_RETRIES: Maximum retry attempts (default: 3)
//   - DLQ_BUFFER_SIZE: DLQ channel buffer size (default: 50)
//   - BACKPRESSURE_MODE: "blocking" or "dropping" (default: "blocking")
//
// Example:
//   export PARTITION_COUNT=5
//   export WORKERS_PER_PARTITION=3
//   export MAX_RETRIES=5
//   export DLQ_BUFFER_SIZE=100
//   export BACKPRESSURE_MODE=dropping
func LoadConsumerGroupConfig() *ConsumerGroupConfig {
	partitionCount := getEnvInt("PARTITION_COUNT", 3)
	workersPerPartition := getEnvInt("WORKERS_PER_PARTITION", 2)
	maxRetries := getEnvInt("MAX_RETRIES", 3)
	dlqBufferSize := getEnvInt("DLQ_BUFFER_SIZE", 50)
	backpressureMode := getEnvString("BACKPRESSURE_MODE", "blocking")

	// Validate configuration
	if partitionCount < 1 {
		fmt.Printf("Warning: PARTITION_COUNT must be >= 1, using default: 3\n")
		partitionCount = 3
	}
	if workersPerPartition < 1 {
		fmt.Printf("Warning: WORKERS_PER_PARTITION must be >= 1, using default: 2\n")
		workersPerPartition = 2
	}
	if maxRetries < 0 {
		fmt.Printf("Warning: MAX_RETRIES must be >= 0, using default: 3\n")
		maxRetries = 3
	}
	if dlqBufferSize < 1 {
		fmt.Printf("Warning: DLQ_BUFFER_SIZE must be >= 1, using default: 50\n")
		dlqBufferSize = 50
	}
	if backpressureMode != "blocking" && backpressureMode != "dropping" {
		fmt.Printf("Warning: BACKPRESSURE_MODE must be 'blocking' or 'dropping', using default: blocking\n")
		backpressureMode = "blocking"
	}

	return &ConsumerGroupConfig{
		PartitionCount:     partitionCount,
		WorkersPerPartition: workersPerPartition,
		MaxRetries:         maxRetries,
		DLQBufferSize:      dlqBufferSize,
		BackpressureMode:   backpressureMode,
	}
}

// getEnvString reads a string from environment variable with a default value
func getEnvString(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// getEnvInt reads an integer from environment variable with a default value
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		fmt.Printf("Warning: Invalid value for %s: %s, using default: %d\n", key, value, defaultValue)
		return defaultValue
	}

	return intValue
}
