package config

import (
	"io"
	"os"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
)

// Option is a function that modifies the configuration.
// It is used to build a Config with flexible options.
type Option func(*Config)

// Config holds the entire application configuration including
// addresses, Redis setup, logging, worker intervals, and metric retention.
type Config struct {
	GRPCAddr    string        // Address for the gRPC server to bind to
	HTTPAddr    string        // Address for the HTTP server (metrics endpoint)
	RedisConfig RedisConfig   // Configuration for connecting to Redis
	Logging     LoggingConfig // Logging level and output settings
	Worker      WorkerConfig  // Configuration for background workers
	Metrics     MetricsConfig // Configuration for metric storage and retention
}

// NewConfig creates a new Config instance and applies all provided Option functions.
// If no options are passed, it returns a default configuration.
func NewConfig(opts ...Option) Config {
	config := newDefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

// WithGrpcAddr sets the gRPC address in the configuration.
func WithGrpcAddr(addr string) Option {
	return func(c *Config) {
		c.GRPCAddr = addr
	}
}

// WithHttpAddr sets the HTTP address in the configuration.
func WithHttpAddr(addr string) Option {
	return func(c *Config) {
		c.HTTPAddr = addr
	}
}

// WithRedisConfig sets the Redis configuration.
func WithRedisConfig(config RedisConfig) Option {
	return func(c *Config) {
		c.RedisConfig = config
	}
}

// WithLoggingConfig sets the logging configuration.
func WithLoggingConfig(config LoggingConfig) Option {
	return func(c *Config) {
		c.Logging = config
	}
}

// WithWorkerConfig sets the worker configuration.
func WithWorkerConfig(config WorkerConfig) Option {
	return func(c *Config) {
		c.Worker = config
	}
}

// WithMetricsConfig sets the metric retention configuration.
func WithMetricsConfig(config MetricsConfig) Option {
	return func(c *Config) {
		c.Metrics = config
	}
}

// newDefaultConfig returns a Config instance with pre-defined default values
// for development and local testing.
func newDefaultConfig() Config {
	redis := RedisConfig{
		Addr:          "localhost:6379",
		Password:      "",
		DB:            1,
		PoolSize:      10,
		StreamName:    "stream",
		ConsumerGroup: "group",
		ConsumerID:    "consumer",
	}
	logging := LoggingConfig{
		Level:  logging.InfoLevel,
		Output: os.Stdout,
	}
	interval := time.Second
	worker := WorkerConfig{
		HeartbeatInterval:        interval,
		ConsumerInterval:         interval,
		RetentionCleanerInterval: interval,
		RedistributorInterval:    interval * 2,
		SinkerInterval:           interval,
		AckerInterval:            interval,
		BatchSize:                1,
	}
	metric := MetricsConfig{
		Retention: interval,
	}

	return Config{
		GRPCAddr:    "127.0.0.1:50051",
		HTTPAddr:    "127.0.0.1:8080",
		RedisConfig: redis,
		Logging:     logging,
		Worker:      worker,
		Metrics:     metric,
	}
}

// RedisConfig holds the configuration for connecting to Redis and
// defining stream and consumer group settings.
type RedisConfig struct {
	Addr          string // Redis server address
	Password      string // Redis password (optional)
	DB            int    // Redis database number
	PoolSize      int    // Maximum number of Redis connections in the pool
	StreamName    string // Redis stream name used for messaging
	ConsumerGroup string // Consumer group name for stream processing
	ConsumerID    string // Consumer ID within the consumer group
}

// LoggingConfig defines logging behavior such as level and output destination.
type LoggingConfig struct {
	Level  logging.Level // Logging level (debug, info, error, etc.)
	Output io.Writer     // Output writer for logs (e.g., os.Stdout)
}

// WorkerConfig defines how each worker behaves — intervals for heartbeats,
// message consumption, redistributing, sink timing, and acking, as well as batch size.
type WorkerConfig struct {
	HeartbeatInterval        time.Duration // Interval between heartbeat updates
	ConsumerInterval         time.Duration // Interval for message consumption
	RedistributorInterval    time.Duration // Interval for redistributing inactive messages
	SinkerInterval           time.Duration // Interval for persisting metrics (if buffered)
	AckerInterval            time.Duration // Interval for acknowledging processed messages
	RetentionCleanerInterval time.Duration // Interval for clean old metrics processing
	BatchSize                int           // Maximum number of messages per consumption batch
	ShutdownTimeout          time.Duration // (Optional) Timeout for graceful shutdown
}

// MetricsConfig contains settings related to in-memory metric retention.
type MetricsConfig struct {
	Retention time.Duration // Duration to retain metrics in memory
}
