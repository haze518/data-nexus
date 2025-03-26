package testutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/pkg/config"
)

type RedisFactory struct {
	t          testing.TB
	logger     *logging.Logger
	baseConfig config.RedisConfig
	prefix     string
}

func NewRedisFactory(t testing.TB, logger *logging.Logger) *RedisFactory {
	prefix := fmt.Sprintf("test:%s:%d", t.Name(), time.Now().UnixNano())

	return &RedisFactory{
		t:          t,
		logger:     logger,
		baseConfig: Config().RedisConfig,
		prefix:     prefix,
	}
}

func (f *RedisFactory) NewBroker(consumerID, consumerGroup string) *broker.RedisBroker {
	cfg := f.baseConfig
	cfg.StreamName = fmt.Sprintf("%s:stream", f.prefix)
	cfg.ConsumerGroup = fmt.Sprintf("%s:%s", f.prefix, consumerGroup)
	cfg.ConsumerID = consumerID

	b, err := broker.NewRedisBroker(cfg, f.logger)
	if err != nil {
		f.t.Fatalf("failed to create broker: %v", err)
	}

	f.t.Cleanup(func() {
		_ = b.Clean()
		_ = b.Close()
	})

	return b
}
