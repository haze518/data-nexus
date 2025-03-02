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

func TestHeartbeater(t *testing.T) {
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	ctx := context.Background()
	testRedisConfig := testutil.TestRedisConfig()
	rs, err := broker.NewRedisBroker(ctx, testRedisConfig, logger)
	if err != nil {
		t.Fatalf("broker.NewRedisBroker: %v", err)
	}

	heartbeater := newHeartbeater(rs, 1*time.Second, logger)
	heartbeater.start(&sync.WaitGroup{})

	time.Sleep(2 * time.Second)

	// check active state
	servers, err := rs.ListServers(context.Background())
	if err != nil {
		t.Fatalf("rs.ListActiveServers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("incorrect number of servers, want: 1, got: %d", len(servers))
	}
	val, ok := servers[testRedisConfig.ConsumerID]
	if !ok {
		t.Fatalf("failed to get server name: %s from servers: %v", testRedisConfig.ConsumerID, servers)
	}
	if val != types.ServerStateActive {
		t.Fatalf("incorrect server state, want: %v, got: %v", types.ServerStateActive, val)
	}

	heartbeater.shutdown()

	time.Sleep(1 * time.Second)

	// check inactive state
	servers, err = rs.ListServers(context.Background())
	if err != nil {
		t.Fatalf("rs.ListActiveServers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("incorrect number of servers, want: 1, got: %d", len(servers))
	}
	val, ok = servers[testRedisConfig.ConsumerID]
	if !ok {
		t.Fatalf("failed to get server name: %s from servers: %v", testRedisConfig.ConsumerID, servers)
	}
	if val != types.ServerStateInactive {
		t.Fatalf("incorrect server state, want: %v, got: %v", types.ServerStateInactive, val)
	}
}
