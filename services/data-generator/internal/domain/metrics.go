package domain

import "time"

// Metrics represents aggregated system performance metrics.
// This is a pure domain model - no infrastructure dependencies.
//
// Business Context:
// - Captures service-level metrics for monitoring and observability
// - Used by monitoring systems, dashboards, and alerting
// - Collected periodically and aggregated over time windows
// - Immutable by design: represents metrics at a point in time
//
// Data Structure Choices:
// 1. Latencies as []float64 (slice)
//    - Ordered time series: need to preserve order for percentile calculations
//    - Sequential processing: calculate p50, p95, p99 from ordered data
//    - Time-based: latencies are collected in sequence
//
// 2. Counters/Labels as map[string]int64 or map[string]string
//    - Fast lookup: O(1) access by metric name
//    - Key-value semantics: metric name -> value
//    - Efficient aggregation: sum counters by key
//
// Tradeoffs:
// - Slice for latencies: Preserves order, allows percentile calculations
//   - Tradeoff: O(n) search if needed, but we typically process all values
// - Map for counters: Fast lookup, natural key-value representation
//   - Tradeoff: No ordering, but counters don't need ordering
type Metrics struct {
	// ServiceName identifies the service these metrics are for
	// Examples: "data-generator", "ml-service", "ingestion-service"
	ServiceName string `json:"service_name"`

	// RequestCount is the total number of requests processed
	RequestCount int64 `json:"request_count"`

	// ErrorCount is the total number of errors encountered
	ErrorCount int64 `json:"error_count"`

	// Latencies is an ordered slice of request latencies in milliseconds
	// Using slice to preserve order for percentile calculations (p50, p95, p99)
	// Example: [10.5, 12.3, 15.2, 11.1, 13.7] - ordered by collection time
	// Slice allows: sort.Float64s(metrics.Latencies) then calculate percentiles
	Latencies []float64 `json:"latencies,omitempty"`

	// CollectedAt is when these metrics were collected
	CollectedAt time.Time `json:"collected_at"`

	// AdditionalCounters are optional key-value counters
	// Examples: {"cache_hits": 1250, "cache_misses": 45, "db_queries": 320}
	// Using map for O(1) lookup and natural key-value semantics
	AdditionalCounters map[string]int64 `json:"additional_counters,omitempty"`

	// Labels are key-value metadata tags for these metrics
	// Examples: {"environment": "production", "region": "us-east-1", "version": "v2.1"}
	// Using map for O(1) lookup and natural key-value semantics
	Labels map[string]string `json:"labels,omitempty"`
}
