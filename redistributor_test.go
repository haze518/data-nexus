package datanexus

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
)

func TestRedistributor(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	client := testutil.SetupRedis(t)
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

	ch := make(chan *types.Metric, 3)
	redistributor := newRedistributor(activeRs, 100*time.Millisecond, logger, ch)

	wg := &sync.WaitGroup{}
	redistributor.start(wg)

	var received []*types.Metric
	for i := 0; i < 3; i++ {
		select {
		case metric := <-ch:
			received = append(received, metric)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout: did not receive expected messages")
		}
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
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
