# Ingestion Service Architecture

## Current Status

The ingestion-service is currently a **placeholder** - it has no implementation yet.

## Service Responsibilities

**ingestion-service** should:
1. **Consume** from Kafka (sensor-data topic)
2. **Process** the consumed events
3. **Store** in DynamoDB (time-series data)
4. **Store** in S3 (optional, for archival)

## Architecture Comparison

### data-generator (Producer)
```
┌─────────────────────┐
│  data-generator     │
│                     │
│  Use Cases:         │
│  - Generate data    │
│  - Publish to Kafka │
│                     │
│  Flow:              │
│  Generate → Publish │
└──────────┬──────────┘
           │
           │ Kafka (publish)
           ▼
```

### ingestion-service (Consumer)
```
           │
           │ Kafka (consume)
           ▼
┌─────────────────────┐
│ ingestion-service  │
│                     │
│  Use Cases:         │
│  - Consume Kafka    │
│  - Process events   │
│  - Store DynamoDB   │
│                     │
│  Flow:              │
│  Consume → Process → Store
└─────────────────────┘
```

## What Should Be Shared vs Separate

### ✅ SHARE: Domain Models
Domain models (SensorEvent, Prediction, Alert, Metrics) should be shared because:
- Both services work with the same data structures
- Ensures consistency across services
- Single source of truth

**Current State:**
- Domain models are in `services/data-generator/internal/domain/`
- **Future:** Should move to shared package (e.g., `pkg/domain/` or separate module)

**For Now:**
- ingestion-service can import from data-generator's domain (temporary)
- Or duplicate (not ideal, but works)
- Or create shared module (best practice)

### ❌ SEPARATE: Use Cases
Each service needs its **own use cases** because they have different responsibilities:

**data-generator use cases:**
- `IngestSensorUseCase` - validates and publishes (producer side)
- `IngestPipelineUseCase` - concurrent publishing pipeline
- `ConsumerGroupUseCase` - publishing with partitions

**ingestion-service use cases (needed):**
- `ConsumeKafkaMessageUseCase` - consumes from Kafka
- `ProcessSensorEventUseCase` - processes consumed events
- `StoreSensorEventUseCase` - stores in DynamoDB/S3
- `KafkaConsumerGroupUseCase` - manages consumer group

## What ingestion-service Needs

### 1. Domain Models (Share or Import)
```go
// Option A: Import from data-generator (temporary)
import "github.com/aegisai/data-generator/internal/domain"

// Option B: Create shared package (better)
import "github.com/aegisai/shared/domain"
```

### 2. Use Cases (Create New)
```
ingestion-service/
├── internal/
│   ├── domain/          # Import from shared or data-generator
│   ├── usecase/         # NEW - ingestion-specific use cases
│   │   ├── consume_kafka.go
│   │   ├── process_event.go
│   │   ├── store_event.go
│   │   └── consumer_group.go
│   └── adapter/         # Infrastructure adapters
│       ├── kafka_consumer.go
│       ├── dynamodb_storage.go
│       └── s3_storage.go
└── cmd/
    └── main.go
```

### 3. Ports (Interfaces)
```go
// Kafka Consumer Port
type KafkaConsumer interface {
    Consume(ctx context.Context) (<-chan domain.SensorEvent, error)
    Commit(ctx context.Context) error
}

// Storage Port (can reuse concept from data-generator)
type EventStorage interface {
    Save(ctx context.Context, event domain.SensorEvent) error
    SaveBatch(ctx context.Context, events []domain.SensorEvent) error
}
```

## Answer to Your Question

**Q: Do we have use cases for ingestion-service?**
**A:** No, not yet. The ingestion-service needs its own use cases.

**Q: Should we move files from data-generator to ingestion-service?**
**A:** No. Here's why:

1. **Different Responsibilities:**
   - data-generator: Producer (generates → publishes)
   - ingestion-service: Consumer (consumes → stores)

2. **Different Use Cases:**
   - data-generator use cases are for publishing
   - ingestion-service needs use cases for consuming

3. **What CAN be shared:**
   - Domain models (SensorEvent, etc.)
   - Storage interface concept (but different implementations)
   - Configuration patterns

## Recommended Structure

### Current (data-generator)
```
data-generator/
├── internal/
│   ├── domain/          # Domain models
│   ├── usecase/         # Publishing use cases
│   └── generator/       # Publishing adapters
```

### Future (ingestion-service)
```
ingestion-service/
├── internal/
│   ├── domain/          # Import shared domain
│   ├── usecase/         # Consuming use cases (NEW)
│   └── adapter/         # Consuming adapters (NEW)
```

### Ideal (Shared Domain)
```
pkg/domain/              # Shared domain models
├── sensor_event.go
├── prediction.go
├── alert.go
└── metrics.go

data-generator/
└── internal/
    └── usecase/         # Publishing use cases

ingestion-service/
└── internal/
    └── usecase/         # Consuming use cases
```

## Next Steps

1. **Keep data-generator as-is** - it's correct for a producer
2. **Create ingestion-service use cases** - new use cases for consuming
3. **Share domain models** - either import or create shared package
4. **Don't move files** - each service has its own responsibilities

## Example: ingestion-service Use Case

```go
// ingestion-service/internal/usecase/consume_kafka.go
package usecase

type ConsumeKafkaMessageUseCase struct {
    consumer KafkaConsumer  // Port interface
    processor ProcessSensorEventUseCase
    logger    Logger
}

func (uc *ConsumeKafkaMessageUseCase) Execute(ctx context.Context) error {
    // Consume from Kafka
    events, err := uc.consumer.Consume(ctx)
    if err != nil {
        return err
    }
    
    // Process each event
    for event := range events {
        if err := uc.processor.Execute(ctx, event); err != nil {
            uc.logger.Error("Failed to process event", err)
            continue
        }
    }
    
    return nil
}
```

This is **different** from data-generator's use cases which are for **publishing**.
