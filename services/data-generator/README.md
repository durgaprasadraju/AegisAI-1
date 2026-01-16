# Data Generator Service

A production-ready microservice for generating and ingesting sensor data, implementing Clean Architecture principles with concurrent processing, Kafka-like consumer groups, and distributed systems patterns.

## Architecture Overview

This service follows **Clean Architecture** with clear layer separation:

```
data-generator/
├── cmd/                    # Application entry points
├── examples/               # Example demonstrations
├── internal/
│   ├── domain/            # Pure business entities (Day 4)
│   ├── usecase/           # Business logic & concurrency (Day 5-6)
│   └── generator/         # Infrastructure adapters (Day 3)
└── README.md              # This file
```

## Key Features

### Day 3: Infrastructure Adapters
- Kafka publisher (local and production)
- DynamoDB storage (LocalStack and AWS)
- Interface-based design for testability

### Day 4: Domain Models
- Pure business entities (SensorEvent, Prediction, Alert, Metrics)
- No infrastructure dependencies
- Reusable across all layers

### Day 5: Concurrent Ingestion Pipeline
- Worker pool pattern
- Buffered channels for event queuing
- Graceful shutdown with context cancellation

### Day 6: Kafka Consumer Group Simulation
- Partition-based parallelism
- Ordering guarantees per partition
- Retry mechanism with exponential backoff
- Dead Letter Queue (DLQ) for failed events
- Backpressure handling (blocking/dropping modes)

## Quick Start

### Prerequisites
- Go 1.23+
- Docker (for Kafka and LocalStack)
- Environment variables (see Configuration)

### Run Examples

**Pipeline Example (Day 5):**
```bash
cd services/data-generator
go run examples/pipeline_example.go
```

**Consumer Group Example (Day 6):**
```bash
cd services/data-generator
go run examples/consumer_group_example.go
```

**Main Example (Day 3):**
```bash
cd services/data-generator
go run cmd/main.go
```

## Configuration

### Environment Variables

**Pipeline Configuration:**
- `WORKER_COUNT`: Number of workers (default: 5)
- `CHANNEL_BUFFER_SIZE`: Channel buffer size (default: 100)

**Consumer Group Configuration:**
- `PARTITION_COUNT`: Number of partitions (default: 3)
- `WORKERS_PER_PARTITION`: Workers per partition (default: 2)
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `DLQ_BUFFER_SIZE`: DLQ channel buffer (default: 50)
- `BACKPRESSURE_MODE`: "blocking" or "dropping" (default: "blocking")

**Kafka Configuration:**
- `KAFKA_BROKER`: Kafka broker address (default: "localhost:9092")
- `KAFKA_TOPIC`: Kafka topic name (default: "sensor-data")

**DynamoDB Configuration:**
- `DYNAMO_ENDPOINT`: DynamoDB endpoint (default: "http://localhost:4566" for LocalStack)
- `AWS_REGION`: AWS region (default: "us-east-1")
- `SENSOR_TABLE`: DynamoDB table name (default: "sensor_readings")

## Directory Structure

### `/cmd`
Application entry points and main functions.

### `/examples`
Example demonstrations showing how to use the service.

### `/internal/domain`
Pure domain models - business entities with no infrastructure dependencies.

### `/internal/usecase`
Business logic layer - use cases, concurrency patterns, and orchestration.

### `/internal/generator`
Infrastructure adapters - Kafka, DynamoDB, and in-memory implementations.

## Development

### Building
```bash
go build ./...
```

### Testing
```bash
go test ./...
```

### Linting
```bash
go vet ./...
```

## Documentation

See README files in each directory for detailed documentation:
- [Domain Models](internal/domain/README.md)
- [Use Cases](internal/usecase/README.md)
- [Generator/Adapters](internal/generator/README.md)
- [Examples](examples/README.md)
- [Commands](cmd/README.md)

## Architecture Principles

1. **Clean Architecture**: Clear separation of concerns
2. **Dependency Inversion**: Depend on interfaces, not implementations
3. **Testability**: Easy to test with mock implementations
4. **Scalability**: Concurrent processing with worker pools
5. **Resilience**: Retry mechanisms and DLQ for failures

## License

[Add your license here]
