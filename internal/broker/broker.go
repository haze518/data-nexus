package broker

import (
	"context"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

type Broker interface {
	Consume(n int64) ([]*types.Metric, error)
	Publish(ctx context.Context, val []byte) (string, error)
	SetServerState(state types.ServerState, ttl time.Duration) error
	ListServers() (map[string]types.ServerState, error)
	MoveInactiveServerMsgs(inactiveSrv string, batchSize int) ([]*types.Metric, error)
	AckCollected(ids ...string) error
	Close() error
}
