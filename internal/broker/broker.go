package broker

import (
	"context"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

type Broker interface {
	SetServerState(ctx context.Context, state types.ServerState, ttl time.Duration) error
}
