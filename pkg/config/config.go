package config

import (
	"io"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
)

type Config struct {
    GRPCAddr    string
    HTTPAddr    string
    RedisConfig RedisConfig
    Logging     LoggingConfig
    Worker      WorkerConfig
    Metrics     MetricsConfig
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
    HeartbeatInterval   time.Duration
    ConsumerInterval    time.Duration
    RedistributorInterval time.Duration
    DataSinkerInterval  time.Duration
    AckerInterval       time.Duration
    BatchSize           int
    ShutdownTimeout     time.Duration
}

type MetricsConfig struct {
    Retention        time.Duration
}
