package kafka

// Config represents Kafka configuration
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

// NewConfig creates a new Kafka configuration
func NewConfig() *Config {
	// TODO: Load from environment or config file
	return &Config{}
}
