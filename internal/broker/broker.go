package broker

import (
	"context"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

type Broker interface {
	Consume(ctx context.Context, n int64) ([]*types.Metric, error)
	Publish(ctx context.Context, val *types.Metric) (string, error)
	SetServerState(ctx context.Context, state types.ServerState, ttl time.Duration) error
	ListServers(ctx context.Context) (map[string]types.ServerState, error)
	MoveInactiveServerMsgs(ctx context.Context, inactiveSrv string, batchSize int) ([]*types.Metric, error)
	AckCollected(ctx context.Context, ids ...string) error
	Close() error
}
