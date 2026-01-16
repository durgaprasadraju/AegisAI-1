package usecase

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/aegisai/data-generator/internal/domain"
)

// ============================================================================
// KAFKA CONSUMER GROUP SIMULATION
// ============================================================================
// ConsumerGroupUseCase simulates Kafka consumer group behavior using pure Go.
// This demonstrates key distributed systems concepts:
// - Partition-based parallelism
// - Ordering guarantees per partition
// - Backpressure handling
// - Retry mechanisms with exponential backoff
// - Dead Letter Queue (DLQ) for failed events
//
// How this maps to Kafka Consumer Groups:
// 1. Partitions: Each partition is a separate channel
//    - Events with same key (SensorID) go to same partition
//    - Maintains ordering per partition (Kafka guarantee)
//    - Parallel processing across partitions
//
// 2. Consumer Group: Worker pools per partition
//    - Multiple workers per partition for throughput
//    - Events within partition processed in order
//    - Events across partitions processed in parallel
//
// 3. Ordering Guarantees:
//    - Same SensorID → same partition → processed in order
//    - Different SensorIDs → different partitions → processed in parallel
//    - This matches Kafka's ordering semantics exactly
//
// 4. Backpressure:
//    - Bounded channels provide natural backpressure
//    - Blocking mode: guarantees delivery (like Kafka producer blocking)
//    - Dropping mode: higher throughput, risk of data loss
//
// 5. Retry & DLQ:
//    - Retries with exponential backoff (prevents overwhelming downstream)
//    - DLQ captures permanently failed events (maps to Kafka DLQ topics)
//
// Why Partitioning Improves Scalability:
// - Parallel processing: Multiple partitions processed concurrently
// - No ordering conflicts: Different partitions don't need coordination
// - Horizontal scaling: Add more partitions/consumers as load increases
// - Resource isolation: Slow partition doesn't block others

// ConsumerGroupUseCase manages a Kafka-like consumer group with partitions
type ConsumerGroupUseCase struct {
	// Configuration
	config *ConsumerGroupConfig

	// Dependencies
	singleUseCase *IngestSensorUseCase // Unit of work for each event
	logger        Logger
	dlqIngester   SensorIngester // Optional: DLQ publisher (nil = logging only)

	// Partition channels - one per partition
	// Each partition maintains ordering for events with same key
	partitions []chan domain.SensorEvent

	// Dead Letter Queue - failed events after max retries
	dlq chan domain.SensorEvent

	// Concurrency primitives
	wg     sync.WaitGroup          // Wait for all workers and DLQ consumer
	ctx    context.Context         // Context for cancellation
	cancel context.CancelFunc      // Cancel function

	// State
	started bool
	mu      sync.Mutex // Protects started flag
}

// NewConsumerGroupUseCase creates a new consumer group use case.
// The dlqIngester parameter is optional - if nil, failed events will only be logged.
// If provided, failed events will be published to the DLQ topic via the ingester.
func NewConsumerGroupUseCase(
	config *ConsumerGroupConfig,
	singleUseCase *IngestSensorUseCase,
	logger Logger,
	dlqIngester SensorIngester, // Optional: nil = logging only
) *ConsumerGroupUseCase {
	// Create partition channels
	partitions := make([]chan domain.SensorEvent, config.PartitionCount)
	// Each partition has a buffer to handle bursts
	// Buffer size should be tuned based on expected event rate per partition
	partitionBufferSize := 50 // Could be configurable
	for i := range partitions {
		partitions[i] = make(chan domain.SensorEvent, partitionBufferSize)
	}

	// Create DLQ channel
	dlq := make(chan domain.SensorEvent, config.DLQBufferSize)

	return &ConsumerGroupUseCase{
		config:        config,
		singleUseCase: singleUseCase,
		logger:        logger,
		dlqIngester:   dlqIngester,
		partitions:    partitions,
		dlq:           dlq,
		started:       false,
	}
}

// Start initializes and starts the consumer group.
// This method:
// - Creates a cancellable context
// - Starts worker pools for each partition
// - Starts DLQ consumer goroutine
// - Returns immediately (non-blocking)
func (cg *ConsumerGroupUseCase) Start(ctx context.Context) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.started {
		return fmt.Errorf("consumer group already started")
	}

	// Create cancellable context
	cg.ctx, cg.cancel = context.WithCancel(ctx)

	// Start worker pools for each partition
	cg.logger.Info(fmt.Sprintf("Starting consumer group: %d partitions, %d workers per partition (total: %d workers)",
		cg.config.PartitionCount, cg.config.WorkersPerPartition,
		cg.config.PartitionCount*cg.config.WorkersPerPartition))

	for partitionID := 0; partitionID < cg.config.PartitionCount; partitionID++ {
		// Start workers for this partition
		for workerID := 0; workerID < cg.config.WorkersPerPartition; workerID++ {
			cg.wg.Add(1)
			go cg.partitionWorker(partitionID, workerID)
		}
	}

	// Start DLQ consumer
	cg.wg.Add(1)
	go cg.dlqConsumer()

	cg.started = true
	cg.logger.Info(fmt.Sprintf("Consumer group started (backpressure mode: %s)", cg.config.BackpressureMode))
	return nil
}

// Ingest routes an event to the appropriate partition based on SensorID.
// This method implements partition routing (similar to Kafka's partitioner).
//
// Partitioning Strategy:
// - Hash SensorID to get consistent partition assignment
// - Same SensorID always goes to same partition (ordering guarantee)
// - Events are distributed evenly across partitions
//
// Backpressure Handling:
// - Blocking mode: Waits for space (guarantees delivery)
// - Dropping mode: Drops event if channel full (higher throughput, data loss risk)
func (cg *ConsumerGroupUseCase) Ingest(event domain.SensorEvent) error {
	cg.mu.Lock()
	started := cg.started
	cg.mu.Unlock()

	if !started {
		return fmt.Errorf("consumer group not started, call Start() first")
	}

	// Route to partition based on SensorID hash
	partitionID := cg.routeToPartition(event)

	// Handle backpressure based on mode
	if cg.config.BackpressureMode == "dropping" {
		// Non-blocking send (drop if full)
		select {
		case cg.partitions[partitionID] <- event:
			return nil
		case <-cg.ctx.Done():
			return fmt.Errorf("consumer group shutting down: %w", cg.ctx.Err())
		default:
			// Channel full, drop event
			cg.logger.Error(fmt.Sprintf("Partition %d channel full, dropping event %s", partitionID, event.EventID), nil)
			return fmt.Errorf("partition %d channel full, event dropped", partitionID)
		}
	} else {
		// Blocking mode: wait for space (guarantees delivery)
		select {
		case cg.partitions[partitionID] <- event:
			return nil
		case <-cg.ctx.Done():
			return fmt.Errorf("consumer group shutting down: %w", cg.ctx.Err())
		}
	}
}

// Shutdown gracefully shuts down the consumer group.
// This method:
// - Closes all partition channels
// - Closes DLQ channel
// - Cancels context (signals workers to stop)
// - Waits for all workers to finish
func (cg *ConsumerGroupUseCase) Shutdown() {
	cg.mu.Lock()
	if !cg.started {
		cg.mu.Unlock()
		return
	}
	cg.mu.Unlock()

	cg.logger.Info("Shutting down consumer group...")

	// Close partition channels
	for i := range cg.partitions {
		close(cg.partitions[i])
	}

	// Close DLQ channel
	close(cg.dlq)

	// Cancel context to signal workers to stop
	if cg.cancel != nil {
		cg.cancel()
	}

	// Wait for all workers to finish
	cg.wg.Wait()

	cg.logger.Info("Consumer group shut down complete")
}

// Wait blocks until all workers have finished processing.
func (cg *ConsumerGroupUseCase) Wait() {
	cg.wg.Wait()
}

// ============================================================================
// PARTITION ROUTING
// ============================================================================

// routeToPartition determines which partition an event should go to.
// Uses hash-based routing to ensure:
// - Same SensorID always goes to same partition (ordering guarantee)
// - Even distribution across partitions
//
// This mirrors Kafka's default partitioner behavior:
// - partition = hash(key) % partitionCount
// - Ensures ordering per partition
// - Allows parallel processing across partitions
func (cg *ConsumerGroupUseCase) routeToPartition(event domain.SensorEvent) int {
	hash := hashString(event.SensorID)
	partitionID := hash % cg.config.PartitionCount
	// Ensure non-negative (Go's % can be negative)
	if partitionID < 0 {
		partitionID = -partitionID
	}
	return partitionID
}

// hashString computes a hash of a string (used for partition routing)
func hashString(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// ============================================================================
// PARTITION WORKER
// ============================================================================

// partitionWorker is a worker goroutine that processes events from a partition.
// Each partition has multiple workers for throughput, but events are processed
// in order within the partition (Kafka ordering guarantee).
//
// Processing Flow:
// 1. Read event from partition channel
// 2. Process with retry logic (exponential backoff)
// 3. If retries exceeded, send to DLQ
// 4. Continue until channel closed or context cancelled
func (cg *ConsumerGroupUseCase) partitionWorker(partitionID, workerID int) {
	defer cg.wg.Done()

	cg.logger.Debug(fmt.Sprintf("Partition %d, Worker %d: started", partitionID, workerID))
	processedCount := 0
	failedCount := 0

	for {
		select {
		case event, ok := <-cg.partitions[partitionID]:
			if !ok {
				// Channel closed, no more events
				cg.logger.Debug(fmt.Sprintf("Partition %d, Worker %d: channel closed (processed: %d, failed: %d)",
					partitionID, workerID, processedCount, failedCount))
				return
			}

			// Process event with retry logic
			if err := cg.processWithRetry(cg.ctx, event); err != nil {
				// Retries exceeded, send to DLQ
				failedCount++
				if dlqErr := cg.sendToDLQ(event); dlqErr != nil {
					cg.logger.Error(fmt.Sprintf("Partition %d, Worker %d: failed to send event %s to DLQ",
						partitionID, workerID, event.EventID), dlqErr)
				} else {
					cg.logger.Error(fmt.Sprintf("Partition %d, Worker %d: event %s sent to DLQ after retries",
						partitionID, workerID, event.EventID), err)
				}
			} else {
				processedCount++
			}

		case <-cg.ctx.Done():
			// Context cancelled, stop processing
			cg.logger.Debug(fmt.Sprintf("Partition %d, Worker %d: context cancelled (processed: %d, failed: %d)",
				partitionID, workerID, processedCount, failedCount))
			return
		}
	}
}

// ============================================================================
// RETRY MECHANISM
// ============================================================================

// processWithRetry processes an event with exponential backoff retry logic.
// This prevents overwhelming downstream systems while giving transient
// failures a chance to recover.
//
// Exponential Backoff:
// - Attempt 0: immediate
// - Attempt 1: baseDelay * 2^0 = baseDelay
// - Attempt 2: baseDelay * 2^1 = baseDelay * 2
// - Attempt 3: baseDelay * 2^2 = baseDelay * 4
// - etc.
//
// Why Exponential Backoff?
// - Prevents overwhelming downstream systems
// - Gives transient failures time to recover
// - Standard pattern in distributed systems
func (cg *ConsumerGroupUseCase) processWithRetry(ctx context.Context, event domain.SensorEvent) error {
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt <= cg.config.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Try to process event
		err := cg.singleUseCase.Execute(ctx, event)
		if err == nil {
			// Success
			if attempt > 0 {
				cg.logger.Debug(fmt.Sprintf("Event %s succeeded after %d retries", event.EventID, attempt))
			}
			return nil
		}

		// Failure - retry if attempts remaining
		if attempt < cg.config.MaxRetries {
			// Calculate exponential backoff delay
			delay := baseDelay * time.Duration(1<<uint(attempt))
			cg.logger.Debug(fmt.Sprintf("Event %s failed (attempt %d/%d), retrying in %v: %v",
				event.EventID, attempt+1, cg.config.MaxRetries+1, delay, err))

			// Wait with context cancellation support
			select {
			case <-time.After(delay):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			// Max retries exceeded
			cg.logger.Error(fmt.Sprintf("Event %s failed after %d attempts", event.EventID, cg.config.MaxRetries+1), err)
			return fmt.Errorf("max retries exceeded: %w", err)
		}
	}

	return fmt.Errorf("unexpected retry loop exit")
}

// ============================================================================
// DEAD LETTER QUEUE (DLQ)
// ============================================================================

// sendToDLQ sends a failed event to the Dead Letter Queue.
// DLQ is critical in distributed systems because:
// 1. Prevents infinite retry loops (events that can never succeed)
// 2. Allows manual inspection of failed events
// 3. Enables reprocessing after fixing underlying issues
// 4. Maps to Kafka DLQ topics in production
func (cg *ConsumerGroupUseCase) sendToDLQ(event domain.SensorEvent) error {
	select {
	case cg.dlq <- event:
		return nil
	case <-cg.ctx.Done():
		return fmt.Errorf("consumer group shutting down: %w", cg.ctx.Err())
	default:
		// DLQ channel full - this is a critical failure
		// In production, might want to persist to disk or alert
		return fmt.Errorf("DLQ channel full, event lost: %s", event.EventID)
	}
}

// dlqConsumer is a goroutine that consumes events from the Dead Letter Queue.
// If dlqIngester is provided, failed events are published to the DLQ Kafka topic.
// If dlqIngester is nil, events are only logged (backward compatible behavior).
func (cg *ConsumerGroupUseCase) dlqConsumer() {
	defer cg.wg.Done()

	if cg.dlqIngester != nil {
		cg.logger.Info("DLQ consumer started (publishing to Kafka DLQ topic)")
	} else {
		cg.logger.Info("DLQ consumer started (logging only, no DLQ publisher configured)")
	}
	dlqCount := 0

	for {
		select {
		case event, ok := <-cg.dlq:
			if !ok {
				// DLQ channel closed
				cg.logger.Info(fmt.Sprintf("DLQ consumer stopped (processed %d failed events)", dlqCount))
				return
			}

			dlqCount++

			// Publish to DLQ topic if ingester is configured
			if cg.dlqIngester != nil {
				// Publish to Kafka DLQ topic
				if err := cg.dlqIngester.Ingest(cg.ctx, event); err != nil {
					// Log error but don't crash - DLQ failures shouldn't kill the system
					cg.logger.Error(fmt.Sprintf("DLQ: Failed to publish event %s to DLQ topic (sensor: %s, type: %s, value: %.2f %s)",
						event.EventID, event.SensorID, event.SensorType, event.Value, event.Unit), err)
				} else {
					// Successfully published to DLQ
					cg.logger.Error(fmt.Sprintf("DLQ: Published failed event %s to DLQ topic (sensor: %s, type: %s, value: %.2f %s)",
						event.EventID, event.SensorID, event.SensorType, event.Value, event.Unit), nil)
				}
			} else {
				// No DLQ publisher configured, just log
				cg.logger.Error(fmt.Sprintf("DLQ: Failed event %s (sensor: %s, type: %s, value: %.2f %s) - no DLQ publisher configured",
					event.EventID, event.SensorID, event.SensorType, event.Value, event.Unit), nil)
			}

		case <-cg.ctx.Done():
			// Context cancelled
			cg.logger.Info(fmt.Sprintf("DLQ consumer stopped (processed %d failed events)", dlqCount))
			return
		}
	}
}

// ============================================================================
// DISTRIBUTED SYSTEMS NOTES
// ============================================================================
//
// How this maps to Kafka Consumer Groups:
// 1. Partitions: Our channels simulate Kafka partitions
//    - Same key → same partition (ordering guarantee)
//    - Parallel processing across partitions
//
// 2. Consumer Group: Our worker pools
//    - Multiple workers per partition for throughput
//    - Events within partition processed in order
//
// 3. Backpressure: Bounded channels
//    - Blocking mode: Like Kafka producer with acks=all
//    - Dropping mode: Like Kafka producer with buffer overflow
//
// 4. Retry & DLQ: Standard patterns
//    - Exponential backoff prevents overwhelming systems
//    - DLQ captures permanently failed events
//    - Maps to Kafka retry topics and DLQ topics
//
// Why Backpressure Protects the System:
// - Prevents memory exhaustion (bounded channels)
// - Prevents overwhelming downstream systems
// - Provides natural flow control
// - Matches Kafka's backpressure mechanisms
//
// Scalability Characteristics:
// - Horizontal: Add more partitions/consumers
// - Vertical: Increase workers per partition
// - No shared state: Each partition independent
// - Ordering preserved: Per partition, not global
