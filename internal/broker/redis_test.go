package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/redis/go-redis/v9"
)

var testRedisConfig = RedisConfig{
	Addr:          "localhost:6379",
	Password:      "",
	DB:            1,
	PoolSize:      10,
	StreamName:    "test_stream",
	ConsumerGroup: "test_group",
	ConsumerID:    "test_consumer",
	MaxStreamLen:  1000,
	TrimStrategy:  "MAXLEN",
	Retention:     10 * time.Minute,
}

func setupRedis(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     testRedisConfig.Addr,
		Password: testRedisConfig.Password,
		DB:       testRedisConfig.DB,
	})
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("client.Ping: %v", err)
	}
	return client
}

func cleanupRedis(t *testing.T, client *redis.Client) {
	err := client.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("client.FlushDB: %v", err)
	}
	client.Close()
}

func TestPublish(t *testing.T) {
	client := setupRedis(t)
	defer cleanupRedis(t, client)

	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	rs, err := NewRedisBroker(ctx, testRedisConfig, logger, "")
	if err != nil {
		t.Fatalf("failed to create stream: %s", err)
	}
	if rs == nil {
		t.Fatalf("redisStream should not be nil")
	}

	err = rs.Publish(ctx, &types.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	streams, err := client.XRange(ctx, testRedisConfig.StreamName, "-", "+").Result()
	if err != nil {
		t.Errorf("client.XRange: %v", err)
	}
	if len(streams) == 0 {
		t.Error("stream should contain messages")
	}
}

func TestConsume(t *testing.T) {
	client := setupRedis(t)
	defer cleanupRedis(t, client)

	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	rs, err := NewRedisBroker(ctx, testRedisConfig, logger, "")
	if err != nil {
		t.Fatalf("failed to create stream: %s", err)
	}
	if rs == nil {
		t.Fatalf("redisStream should not be nil")
	}

	metric := &types.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Timestamp: time.Now(),
	}

	err = rs.Publish(ctx, metric)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	metrics, err := rs.Consume(ctx, 1)
	if err != nil {
		t.Fatalf("failed to consume message: %v", err)
	}

	if len(metrics) != 1 {
		t.Errorf("expected to consume 1 message, got %d", len(metrics))
	}

	if metrics[0].Name != metric.Name {
		t.Errorf("expected metric name %q, got %q", metric.Name, metrics[0].Name)
	}

	if metrics[0].Value != metric.Value {
		t.Errorf("expected metric value %f, got %f", metric.Value, metrics[0].Value)
	}
}
