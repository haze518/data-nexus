package datanexus

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/redis/go-redis/v9"
)

func TestRedistributor_ProcessInactiveServers_MessagesReassigned(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	client := testutil.SetupRedis(t)
	config := testutil.TestRedisConfig()
	defer testutil.CleanupRedis(t, client)

	inactiveRs := newRedis(ctx, "dead_server", logger)
	activeRs := newRedis(ctx, "active_server", logger)

	err := inactiveRs.SetServerState(ctx, types.ServerStateInactive, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to set server state: %v", err)
	}

	for i := 0; i < 3; i++ {
		err = inactiveRs.Publish(ctx, &types.Metric{
			Name:      "cpu_usage",
			Value:     42.5 + float64(i),
			Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	_, err = inactiveRs.Consume(ctx, 3)
	if err != nil {
		t.Fatalf("failed to consume messages: %v", err)
	}

	redistributor := newRedistributor(activeRs, 100*time.Millisecond, logger)
	wg := &sync.WaitGroup{}
	redistributor.start(wg)

	time.Sleep(1 * time.Second)

	pendingActive, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   config.StreamName,
		Group:    config.ConsumerGroup,
		Start:    "-",
		End:      "+",
		Count:    10,
		Consumer: "active_server",
	}).Result()
	if err != nil {
		t.Fatalf("failed to get pending messages for active_server: %v", err)
	}
	if len(pendingActive) == 0 {
		t.Errorf("expected pending messages for active_server, but got none")
	}

	pendingDead, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   config.StreamName,
		Group:    config.ConsumerGroup,
		Start:    "-",
		End:      "+",
		Count:    10,
		Consumer: "dead_server",
	}).Result()
	if err != nil {
		t.Fatalf("failed to get pending messages for dead_server: %v", err)
	}
	if len(pendingDead) > 0 {
		t.Errorf("expected no pending messages for dead_server, but got %d", len(pendingDead))
	}

	exists, _ := client.Exists(ctx, fmt.Sprintf("heartbeat:%s", "dead_server")).Result()
	if exists != 0 {
		t.Errorf("expected heartbeat to be deleted after all messages were moved, but it still exists")
	}

	redistributor.shutdown()
	wg.Wait()
}

func newRedis(ctx context.Context, name string, logger *logging.Logger) *broker.RedisBroker {
	testRedisConfig := testutil.TestRedisConfig()
	testRedisConfig.ConsumerID = name
	rs, err := broker.NewRedisBroker(ctx, testRedisConfig, logger)
	if err != nil {
		panic("failed to create stream")
	}
	return rs
}
