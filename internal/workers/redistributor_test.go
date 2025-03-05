package workers

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
	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/protobuf/proto"
)

func TestRedistributor(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	inactiveRs := newRedis("dead_server", logger)
	activeRs := newRedis("active_server", logger)

	err := inactiveRs.SetServerState(types.ServerStateInactive, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to set server state: %v", err)
	}

	for i := 0; i < 3; i++ {
		metric, _ := proto.Marshal(&pb.Metric{
			Name:      "cpu_usage",
			Value:     42.5 + float64(i),
			Timestamp: time.Now().Unix(),
		})
		_, err = inactiveRs.Publish(ctx, metric)
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	_, err = inactiveRs.Consume(3)
	if err != nil {
		t.Fatalf("failed to consume messages: %v", err)
	}

	ch := make(chan []*types.Metric, 1)
	redistributor := NewRedistributor(activeRs, 100*time.Millisecond, logger, ch)

	var wg sync.WaitGroup
	redistributor.Start(&wg)

	var received []*types.Metric
	select {
	case received = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: did not receive expected messages")
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}

	redistributor.Shutdown()
	wg.Wait()
}

func newRedis(name string, logger *logging.Logger) *broker.RedisBroker {
	testRedisConfig := testutil.Config().RedisConfig
	testRedisConfig.ConsumerID = name
	rs, err := broker.NewRedisBroker(testRedisConfig, logger)
	if err != nil {
		panic("failed to create stream")
	}
	return rs
}
