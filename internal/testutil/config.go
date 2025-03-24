package testutil

import (
	"os"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/pkg/config"
)

func Config() config.Config {
	redis := config.RedisConfig{
		Addr:          "localhost:6379",
		Password:      "",
		DB:            1,
		PoolSize:      10,
		StreamName:    "test_stream",
		ConsumerGroup: "test_group",
		ConsumerID:    "test_consumer",
	}
	logging := config.LoggingConfig{
		Level:  logging.InfoLevel,
		Output: os.Stdout,
	}
	interval := time.Millisecond * 10
	worker := config.WorkerConfig{
		HeartbeatInterval:        interval,
		ConsumerInterval:         interval,
		RedistributorInterval:    interval * 2,
		SinkerInterval:           interval,
		AckerInterval:            interval,
		RetentionCleanerInterval: interval,
		BatchSize:                100,
	}
	metric := config.MetricsConfig{
		Retention: interval,
	}
	return config.Config{
		GRPCAddr:    "127.0.0.1:50051",
		HTTPAddr:    "127.0.0.1:8080",
		RedisConfig: redis,
		Logging:     logging,
		Worker:      worker,
		Metrics:     metric,
	}
}
