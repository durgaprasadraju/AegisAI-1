package metrics

// Collector interface for metrics collection
type Collector interface {
	IncrementCounter(name string, tags map[string]string)
	RecordHistogram(name string, value float64, tags map[string]string)
}

// DefaultCollector is a default metrics collector implementation
type DefaultCollector struct{}

func (c *DefaultCollector) IncrementCounter(name string, tags map[string]string) {}
func (c *DefaultCollector) RecordHistogram(name string, value float64, tags map[string]string) {}
