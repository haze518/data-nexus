package config

import (
	"io"
	"os"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
)

type Option func(*Config)

type Config struct {
	GRPCAddr    string
	HTTPAddr    string
	RedisConfig RedisConfig
	Logging     LoggingConfig
	Worker      WorkerConfig
	Metrics     MetricsConfig
}

func NewConfig(opts ...Option) Config {
	config := newDefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

func WithGrpcAddr(addr string) Option {
	return func(c *Config) {
		c.GRPCAddr = addr
	}
}

func WithHttpAddr(addr string) Option {
	return func(c *Config) {
		c.HTTPAddr = addr
	}
}

func WithRedisConfig(config RedisConfig) Option {
	return func(c *Config) {
		c.RedisConfig = config
	}
}

func WithLoggingConfig(config LoggingConfig) Option {
	return func(c *Config) {
		c.Logging = config
	}
}

func WithWorkerConfig(config WorkerConfig) Option {
	return func(c *Config) {
		c.Worker = config
	}
}

func WithMetricsConfig(config MetricsConfig) Option {
	return func(c *Config) {
		c.Metrics = config
	}
}

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
		HeartbeatInterval:     interval,
		ConsumerInterval:      interval,
		RedistributorInterval: interval * 2,
		SinkerInterval:        interval,
		AckerInterval:         interval,
		BatchSize:             1,
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

type RedisConfig struct {
	Addr          string
	Password      string
	DB            int
	PoolSize      int
	StreamName    string
	ConsumerGroup string
	ConsumerID    string
}

type LoggingConfig struct {
	Level  logging.Level
	Output io.Writer
}

type WorkerConfig struct {
	HeartbeatInterval     time.Duration
	ConsumerInterval      time.Duration
	RedistributorInterval time.Duration
	SinkerInterval        time.Duration
	AckerInterval         time.Duration
	BatchSize             int
	ShutdownTimeout       time.Duration
}

type MetricsConfig struct {
	Retention time.Duration
}
