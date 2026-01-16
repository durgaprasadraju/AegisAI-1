# Examples

Example demonstrations showing how to use the data-generator service with different concurrency patterns.

## Files

### `pipeline_example.go` - Simple Worker Pool Example (Day 5)
Demonstrates the concurrent ingestion pipeline using worker pool pattern.

**What it shows:**
1. Creating a concurrent ingestion pipeline
2. Generating and submitting 20 sensor events
3. Worker pool processing events concurrently
4. Graceful shutdown handling

**Key Concepts:**
- Worker pool pattern
- Buffered channels
- Context cancellation
- Error handling

**To run:**
```bash
cd services/data-generator
go run -tags pipeline_example ./examples/pipeline_example.go
```

**Output:**
- Shows event submission
- Demonstrates concurrent processing
- Shows graceful shutdown

**Mock Implementation:**
- `MockIngester`: Simple mock that simulates processing
- Can be replaced with real Kafka/DynamoDB adapters

### `consumer_group_example.go` - Kafka Consumer Group Example (Day 6)
Demonstrates Kafka-like consumer group with partitions, retries, and DLQ.

**What it shows:**
1. Partition-based routing (same SensorID → same partition)
2. Concurrent processing across partitions
3. Retry mechanism with exponential backoff
4. Dead Letter Queue for failed events
5. Backpressure handling

**Key Concepts:**
- Partition routing (hash-based)
- Ordering guarantees per partition
- Retry with exponential backoff
- Dead Letter Queue
- Backpressure modes

**To run:**
```bash
cd services/data-generator
go run -tags consumer_group_example ./examples/consumer_group_example.go
```

**Output:**
- Shows partition routing consistency
- Demonstrates retry behavior
- Shows DLQ for failed events
- Explains how it maps to Kafka

**Mock Implementation:**
- `FailingMockIngester`: Randomly fails events to demonstrate retry/DLQ
- 15% failure rate for demonstration
- Can be replaced with real adapters

## Configuration

Examples use default configuration but can be customized via environment variables:

```bash
# Pipeline configuration
export WORKER_COUNT=10
export CHANNEL_BUFFER_SIZE=200

# Consumer group configuration
export PARTITION_COUNT=5
export WORKERS_PER_PARTITION=3
export MAX_RETRIES=5
export DLQ_BUFFER_SIZE=100
export BACKPRESSURE_MODE=dropping
```

## Mock Implementations

Both examples use mock implementations of `SensorIngester`:

1. **MockIngester** (pipeline_example.go):
   - Simple mock that always succeeds
   - Simulates processing time (100ms)
   - Logs successful processing

2. **FailingMockIngester** (consumer_group_example.go):
   - Randomly fails events (configurable failure rate)
   - Demonstrates retry mechanism
   - Shows DLQ behavior

**In Production:**
Replace mocks with real adapters:
- Kafka adapter: Publishes to Kafka topic
- DynamoDB adapter: Saves to database
- HTTP adapter: Sends to REST API

## Learning Path

1. **Start with pipeline_example.go**:
   - Understand worker pool pattern
   - Learn about channels and goroutines
   - See graceful shutdown

2. **Then try consumer_group_example.go**:
   - Understand partitioning
   - Learn about retry mechanisms
   - See DLQ in action

3. **Modify and experiment**:
   - Change configuration values
   - Adjust failure rates
   - Add your own logic

## Key Takeaways

### From Pipeline Example:
- Worker pools control concurrency
- Channels provide thread-safe communication
- Context enables graceful shutdown
- Errors don't crash the system

### From Consumer Group Example:
- Partitioning enables parallel processing
- Same key → same partition (ordering)
- Retries handle transient failures
- DLQ captures permanent failures
- Backpressure protects the system

## Next Steps

After understanding these examples:
1. Create your own adapters (Kafka, DynamoDB)
2. Integrate with real infrastructure
3. Add monitoring and metrics
4. Scale horizontally
