package workers

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
)

func TestHeartbeater(t *testing.T) {
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	factory := testutil.NewRedisFactory(t, logger)
	rs := factory.NewBroker("", "heartbeater")

	heartbeater := NewHeartbeater(rs, 1*time.Second, logger)
	heartbeater.Start(&sync.WaitGroup{})

	time.Sleep(2 * time.Second)

	// check active state
	servers, err := rs.ListServers()
	if err != nil {
		t.Fatalf("rs.ListActiveServers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("incorrect number of servers, want: 1, got: %d", len(servers))
	}
	val, ok := servers[rs.Config.ConsumerID]
	if !ok {
		t.Fatalf("failed to get server name: %s from servers: %v", rs.Config.ConsumerID, servers)
	}
	if val != types.ServerStateActive {
		t.Fatalf("incorrect server state, want: %v, got: %v", types.ServerStateActive, val)
	}

	heartbeater.Shutdown()

	time.Sleep(1 * time.Second)

	// check inactive state
	servers, err = rs.ListServers()
	if err != nil {
		t.Fatalf("rs.ListActiveServers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("incorrect number of servers, want: 1, got: %d", len(servers))
	}
	val, ok = servers[rs.Config.ConsumerID]
	if !ok {
		t.Fatalf("failed to get server name: %s from servers: %v", rs.Config.ConsumerID, servers)
	}
	if val != types.ServerStateInactive {
		t.Fatalf("incorrect server state, want: %v, got: %v", types.ServerStateInactive, val)
	}
}
