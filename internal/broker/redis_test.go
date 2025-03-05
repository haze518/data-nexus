package broker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
)

func TestPublish(t *testing.T) {
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	testRedisConfig := testutil.TestRedisConfig()
	rs, err := NewRedisBroker(ctx, testRedisConfig, logger)
	if err != nil {
		t.Fatalf("failed to create stream: %s", err)
	}
	if rs == nil {
		t.Fatalf("redisStream should not be nil")
	}

	_, err = rs.Publish(ctx, &types.Metric{
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
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	testRedisConfig := testutil.TestRedisConfig()
	rs, err := NewRedisBroker(ctx, testRedisConfig, logger)
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

	_, err = rs.Publish(ctx, metric)
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

func TestMoveInactiveServerMsgs(t *testing.T) {
	ctx := context.Background()
	batchSize := 50
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)

	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	inactiveRs := newRedis(ctx, "dead_server", logger)
	activeRs := newRedis(ctx, "active_server", logger)

	for i := 0; i < 3; i++ {
		_, err := inactiveRs.Publish(ctx, &types.Metric{
			Name:      "cpu_usage",
			Value:     float64(i) + 42.5,
			Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	err := inactiveRs.SetServerState(ctx, types.ServerStateInactive, 5*time.Second)
	if err != nil {
		t.Fatalf("inactiveRs.SetServerState: %v", err)
	}

	_, err = inactiveRs.Consume(ctx, 3)
	if err != nil {
		t.Fatalf("failed to consume messages: %v", err)
	}

	messageIDs, err := activeRs.MoveInactiveServerMsgs(ctx, "dead_server", batchSize)
	if err != nil {
		t.Fatalf("MoveInactiveServerMsgs failed: %v", err)
	}
	if len(messageIDs) == 0 {
		t.Errorf("expected to move messages, but got none")
	}

	exists, _ := client.Exists(ctx, fmt.Sprintf("state:%s", "dead_server")).Result()
	if exists == 0 {
		t.Error("expected state to exist, but it was deleted early")
	}

	messageIDs, err = activeRs.MoveInactiveServerMsgs(ctx, "dead_server", batchSize)
	if len(messageIDs) != 0 {
		t.Error("incorrect number of messageIDs, should be 0")
	}
	if err != nil {
		t.Fatalf("MoveInactiveServerMsgs failed: %v", err)
	}

	exists, _ = client.Exists(ctx, fmt.Sprintf("state:%s", "dead_server")).Result()
	if exists != 0 {
		t.Errorf("expected state to be deleted after all messages were moved")
	}
}

func newRedis(ctx context.Context, name string, logger *logging.Logger) *RedisBroker {
	testRedisConfig := testutil.TestRedisConfig()
	testRedisConfig.ConsumerID = name
	rs, err := NewRedisBroker(ctx, testRedisConfig, logger)
	if err != nil {
		panic("failed to create stream")
	}
	return rs
}
