package broker

import (
	"context"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

type Broker interface {
	SetServerState(ctx context.Context, state types.ServerState, ttl time.Duration) error
	ListServers(ctx context.Context) (map[string]types.ServerState, error)
	MoveInactiveServerMsgs(ctx context.Context, inactiveSrv string, batchSize int) ([]string, error)
}
