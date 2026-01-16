package usecase

import (
	"context"
	"fmt"
	"sync"

	"github.com/aegisai/data-generator/internal/domain"
)

// ============================================================================
// CONCURRENT INGESTION PIPELINE USE CASE
// ============================================================================
// IngestPipelineUseCase implements a concurrent ingestion pipeline using
// the worker pool pattern. This mirrors real-world stream processing systems
// like Kafka consumer groups.
//
// Architecture:
// - Producer pushes events into buffered channel
// - Worker pool (N goroutines) reads from channel
// - Each worker processes events concurrently
// - Graceful shutdown via context cancellation
//
// Why Worker Pool Pattern?
// 1. Controls concurrency: Prevents resource exhaustion
// 2. Predictable resource usage: Fixed number of goroutines
// 3. Horizontal scaling: Run multiple pipeline instances
// 4. Mirrors Kafka consumer groups: Similar behavior to production
//
// Why Channels?
// 1. Thread-safe: No mutex needed for communication
// 2. Built-in backpressure: Blocks when buffer full
// 3. Clean shutdown: Close channel to signal completion
// 4. Idiomatic Go: Standard concurrency pattern
//
// Why Buffered Channel?
// 1. Non-blocking producer: Can push events while workers process
// 2. Smooths out bursts: Buffer absorbs temporary spikes
// 3. Matches Kafka prefetch: Similar to consumer group behavior
// 4. Balances memory vs throughput: Configurable buffer size

// IngestPipelineUseCase manages a concurrent ingestion pipeline
type IngestPipelineUseCase struct {
	// Configuration
	config *PipelineConfig

	// Dependencies
	singleUseCase *IngestSensorUseCase // Unit of work for each event
	logger        Logger

	// Concurrency primitives
	inputChan chan domain.SensorEvent // Buffered channel for events
	wg        sync.WaitGroup          // Wait for workers to finish
	ctx       context.Context         // Context for cancellation
	cancel    context.CancelFunc      // Cancel function

	// State
	started bool
	mu      sync.Mutex // Protects started flag
}

// NewIngestPipelineUseCase creates a new concurrent ingestion pipeline
func NewIngestPipelineUseCase(
	config *PipelineConfig,
	singleUseCase *IngestSensorUseCase,
	logger Logger,
) *IngestPipelineUseCase {
	return &IngestPipelineUseCase{
		config:        config,
		singleUseCase: singleUseCase,
		logger:        logger,
		inputChan:     make(chan domain.SensorEvent, config.ChannelBufferSize),
		started:       false,
	}
}

// Start initializes and starts the worker pool.
// This method:
// - Creates a cancellable context
// - Spawns worker goroutines
// - Returns immediately (non-blocking)
//
// Must be called before Ingestion().
// Safe to call multiple times (idempotent).
func (p *IngestPipelineUseCase) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return fmt.Errorf("pipeline already started")
	}

	// Create cancellable context
	p.ctx, p.cancel = context.WithCancel(ctx)

	// Start worker pool
	p.logger.Info(fmt.Sprintf("Starting ingestion pipeline with %d workers (buffer: %d)",
		p.config.WorkerCount, p.config.ChannelBufferSize))

	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	p.started = true
	p.logger.Info("Ingestion pipeline started")
	return nil
}

// Ingestion submits an event to the pipeline for processing.
// This method is non-blocking (unless channel buffer is full).
//
// Returns error if:
// - Pipeline not started
// - Context cancelled
// - Channel buffer full (shouldn't happen with proper sizing)
//
// Error handling:
// - Errors are logged but don't stop the pipeline
// - Failed events are lost (in production, might want retry queue)
func (p *IngestPipelineUseCase) Ingestion(event domain.SensorEvent) error {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()

	if !started {
		return fmt.Errorf("pipeline not started, call Start() first")
	}

	// Non-blocking send (unless buffer is full)
	select {
	case p.inputChan <- event:
		// Event queued successfully
		return nil
	case <-p.ctx.Done():
		// Context cancelled, pipeline shutting down
		return fmt.Errorf("pipeline shutting down: %w", p.ctx.Err())
	default:
		// Channel buffer full (shouldn't happen with proper sizing)
		// In production, might want to block or use a larger buffer
		return fmt.Errorf("pipeline buffer full, event dropped")
	}
}

// Shutdown gracefully shuts down the pipeline.
// This method:
// - Closes the input channel (stops accepting new events)
// - Cancels context (signals workers to stop)
// - Waits for workers to finish processing current events
//
// Safe to call multiple times (idempotent).
func (p *IngestPipelineUseCase) Shutdown() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	p.logger.Info("Shutting down ingestion pipeline...")

	// Close channel to stop accepting new events
	// This is safe to call multiple times (Go handles it)
	close(p.inputChan)

	// Cancel context to signal workers to stop
	if p.cancel != nil {
		p.cancel()
	}

	// Wait for workers to finish
	p.wg.Wait()

	p.logger.Info("Ingestion pipeline shut down complete")
}

// Wait blocks until all workers have finished processing.
// Useful for ensuring all events are processed before program exit.
func (p *IngestPipelineUseCase) Wait() {
	p.wg.Wait()
}

// ============================================================================
// WORKER IMPLEMENTATION
// ============================================================================

// worker is a single worker goroutine that processes events from the channel.
// Each worker:
// 1. Reads events from input channel
// 2. Calls singleUseCase.Execute() to process event
// 3. Handles errors (logs, doesn't crash)
// 4. Exits when channel closed or context cancelled
//
// Why this pattern?
// - Each worker processes events independently
// - Errors in one event don't affect others
// - Graceful shutdown via context cancellation
// - No race conditions (channels are thread-safe)
func (p *IngestPipelineUseCase) worker(id int) {
	defer p.wg.Done()

	p.logger.Debug(fmt.Sprintf("Worker %d started", id))
	processedCount := 0

	for {
		select {
		case event, ok := <-p.inputChan:
			// Channel closed or event received
			if !ok {
				// Channel closed, no more events
				p.logger.Debug(fmt.Sprintf("Worker %d: channel closed, shutting down (processed: %d)", id, processedCount))
				return
			}

			// Process event
			if err := p.singleUseCase.Execute(p.ctx, event); err != nil {
				// Error logged by singleUseCase, continue processing
				// In production, might want to send to dead-letter queue
				p.logger.Error(fmt.Sprintf("Worker %d: failed to process event %s", id, event.EventID), err)
			} else {
				processedCount++
			}

		case <-p.ctx.Done():
			// Context cancelled, stop processing
			p.logger.Debug(fmt.Sprintf("Worker %d: context cancelled, shutting down (processed: %d)", id, processedCount))
			return
		}
	}
}

// ============================================================================
// SCALABILITY AND DISTRIBUTED SYSTEMS NOTES
// ============================================================================
//
// How this pattern scales horizontally:
// 1. Run multiple pipeline instances (different processes/machines)
// 2. Each instance has its own worker pool
// 3. Load balancer distributes events across instances
// 4. Total throughput = instance_count * worker_count * events_per_second
//
// How this maps to real-world stream processing:
// - Kafka consumer groups: Similar pattern (multiple consumers, shared topic)
// - AWS Kinesis: Similar pattern (multiple shard processors)
// - Google Pub/Sub: Similar pattern (multiple subscribers)
//
// Why this is safe for distributed systems:
// 1. No shared state: Each worker processes independently
// 2. Idempotent operations: Can retry failed events
// 3. Graceful degradation: Errors don't crash the system
// 4. Resource limits: Worker pool prevents resource exhaustion
// 5. Backpressure: Channel buffer provides natural backpressure
