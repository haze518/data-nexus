package broker_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/protobuf/proto"
)

func TestPublish(t *testing.T) {
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	factory := testutil.NewRedisFactory(t, logger)
	rs := factory.NewBroker("", "publish")

	metric, _ := proto.Marshal(&pb.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Timestamp: time.Now().Unix(),
	})
	_, err := rs.Publish(ctx, metric)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	streams, err := rs.Client.XRange(ctx, rs.Config.StreamName, "-", "+").Result()
	if err != nil {
		t.Errorf("client.XRange: %v", err)
	}
	if len(streams) == 0 {
		t.Error("stream should contain messages")
	}
}

func TestConsume(t *testing.T) {
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	factory := testutil.NewRedisFactory(t, logger)
	rs := factory.NewBroker("", "consume")

	metric := &pb.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Timestamp: time.Now().Unix(),
	}
	pbmetric, _ := proto.Marshal(metric)
	_, err := rs.Publish(ctx, pbmetric)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	metrics, err := rs.Consume(1)
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

	factory := testutil.NewRedisFactory(t, logger)

	inactiveRs := factory.NewBroker("dead_server", "mv_inactive_server")
	activeRs := factory.NewBroker("active_server", "mv_inactive_server")

	for i := 0; i < 3; i++ {
		metric, _ := proto.Marshal(&pb.Metric{
			Name:      "cpu_usage",
			Value:     float64(i) + 42.5,
			Timestamp: time.Now().Unix(),
		})
		_, err := inactiveRs.Publish(ctx, metric)
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	err := inactiveRs.SetServerState(types.ServerStateInactive, 5*time.Second)
	if err != nil {
		t.Fatalf("inactiveRs.SetServerState: %v", err)
	}

	_, err = inactiveRs.Consume(3)
	if err != nil {
		t.Fatalf("failed to consume messages: %v", err)
	}

	messageIDs, err := activeRs.MoveInactiveServerMsgs("dead_server", batchSize)
	if err != nil {
		t.Fatalf("MoveInactiveServerMsgs failed: %v", err)
	}
	if len(messageIDs) == 0 {
		t.Errorf("expected to move messages, but got none")
	}

	exists, _ := activeRs.Client.Exists(ctx, fmt.Sprintf("state:%s:%s", activeRs.Config.Namespace(), "dead_server")).Result()
	if exists == 0 {
		t.Error("expected state to exist, but it was deleted early")
	}

	messageIDs, err = activeRs.MoveInactiveServerMsgs("dead_server", batchSize)
	if len(messageIDs) != 0 {
		t.Error("incorrect number of messageIDs, should be 0")
	}
	if err != nil {
		t.Fatalf("MoveInactiveServerMsgs failed: %v", err)
	}

	exists, _ = activeRs.Client.Exists(ctx, fmt.Sprintf("state:%s:%s", activeRs.Config.Namespace(), "dead_server")).Result()
	if exists != 0 {
		t.Errorf("expected state to be deleted after all messages were moved")
	}
}
