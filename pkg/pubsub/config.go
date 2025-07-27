package pubsub

type Config struct {
	MaxWorkers int `json:"max_workers"`
}

func DefaultConfig() *Config {
	return &Config{
		MaxWorkers: 10,
	}
}
