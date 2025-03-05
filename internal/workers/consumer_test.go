package workers

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
	"google.golang.org/protobuf/proto"
	pb "github.com/haze518/data-nexus/proto"
)

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	rs := newRedis("srv", logger)
	var wg sync.WaitGroup
	ch := make(chan []*types.Metric, 1)
	consumer := NewConsumer(rs, 100*time.Millisecond, logger, ch, 3)
	consumer.Start(&wg)

	for i := 0; i < 3; i++ {
		metric, _ := proto.Marshal(&pb.Metric{
			Name:      "cpu_usage",
			Value:     42.5 + float64(i),
			Timestamp: time.Now().Unix(),
		})
		_, err := rs.Publish(ctx, metric)
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	var received []*types.Metric
	select {
	case received = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: did not receive expected messages")
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}

	consumer.Shutdown()
	wg.Wait()
}
