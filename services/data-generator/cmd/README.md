# Command Entry Points

Application entry points and main functions for the data-generator service.

## Files

### `main.go` - Day 3 Example
Demonstrates the foundation of AegisAI with Kafka and DynamoDB integration using interfaces.

**What it shows:**
1. Dependency Inversion Principle
2. Interface-based design
3. Kafka integration (local Docker Kafka)
4. DynamoDB integration (LocalStack)
5. Same code works in production with only config changes

**Key Concepts:**
- Interfaces (SensorGenerator, Publisher, Storage)
- Dependency injection
- Local vs production (only config changes)
- Pure functions
- Batch operations

**To run:**
```bash
cd services/data-generator
go run cmd/main.go
```

**Prerequisites:**
- Kafka running (Docker): `docker-compose -f docker/docker-compose.kafka.yml up -d`
- LocalStack running (Docker): `docker run -d -p 4566:4566 localstack/localstack`

**What it demonstrates:**
1. **Step 1**: Initialize implementations (generators, publisher, storage)
2. **Step 2**: Generate sensor readings using interfaces
3. **Step 3**: Demonstrate pure functions
4. **Step 4**: Publish readings using Publisher interface
5. **Step 5**: Store readings using Storage interface
6. **Step 6**: Retrieve latest readings

**Key Insight:**
All variables are interfaces, not concrete types. This means:
- Swap implementations without changing code
- Test with mock implementations
- Gradually migrate from local to production
- Same code, different configurations

## Architecture

This entry point demonstrates:
- **Clean Architecture**: Use cases depend on interfaces
- **Dependency Inversion**: High-level modules don't depend on low-level modules
- **Testability**: Easy to test with mocks
- **Flexibility**: Swap implementations without code changes

## Configuration

Uses environment variables with defaults:
- `KAFKA_BROKER`: Kafka broker (default: "localhost:9092")
- `KAFKA_TOPIC`: Kafka topic (default: "sensor-data")
- `DYNAMO_ENDPOINT`: DynamoDB endpoint (default: "http://localhost:4566")
- `AWS_REGION`: AWS region (default: "us-east-1")
- `SENSOR_TABLE`: DynamoDB table (default: "sensor_readings")

## Migration Path

**Day 2 → Day 3:**
- Day 2: In-memory implementations
- Day 3: Kafka + DynamoDB adapters (same interfaces)

**Local → Production:**
- Local: Kafka (Docker) + LocalStack
- Production: AWS MSK + Real DynamoDB
- **No code changes needed!** Only environment variables

## Example Output

The program demonstrates:
- Interface usage (not concrete types)
- Pure functions (stateless)
- Batch operations
- Error handling
- Dependency inversion principle

## Next Steps

After running this example:
1. Try the pipeline example (`examples/pipeline_example.go`)
2. Try the consumer group example (`examples/consumer_group_example.go`)
3. Create your own adapters
4. Integrate with production infrastructure
