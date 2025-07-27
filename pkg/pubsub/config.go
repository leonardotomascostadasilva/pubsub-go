package pubsub

import (
	"time"
)

type Config struct {
	MaxRetries    int           `json:"max_retries"`
	RetryDelay    time.Duration `json:"retry_delay"`
	MaxWorkers    int           `json:"max_workers"`
	BufferSize    int           `json:"buffer_size"`
	EnableMetrics bool          `json:"enable_metrics"`
	EnableTracing bool          `json:"enable_tracing"`
}

func DefaultConfig() *Config {
	return &Config{
		MaxRetries:    3,
		RetryDelay:    time.Second * 2,
		MaxWorkers:    10,
		BufferSize:    100,
		EnableMetrics: true,
		EnableTracing: false,
	}
}
