package broker

import (
	"context"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

// Broker defines the interface for message broker operations used in the data pipeline.
type Broker interface {
	// Consume retrieves up to 'n' metrics from the broker.
	// Returns a slice of Metric pointers or an error.
	Consume(n int64) ([]*types.Metric, error)

	// Publish sends a single encoded message (e.g., metric payload) to the broker.
	// Returns the assigned message ID or an error.
	Publish(ctx context.Context, val []byte) (string, error)

	// PublishBatch sends multiple encoded messages to the broker in one operation.
	// Returns a slice of message IDs or an error.
	PublishBatch(ctx context.Context, vals [][]byte) ([]string, error)

	// SetServerState sets the current server's state (e.g., active or inactive)
	// with a time-to-live (TTL) duration after which the state expires.
	SetServerState(state types.ServerState, ttl time.Duration) error

	// ListServers returns a map of server IDs to their current state.
	// This is used for discovering active and inactive peers.
	ListServers() (map[string]types.ServerState, error)

	// MoveInactiveServerMsgs transfers messages from the stream of an inactive server
	// into the current server's processing queue.
	// Returns up to batchSize metrics or an error.
	MoveInactiveServerMsgs(inactiveSrv string, batchSize int) ([]*types.Metric, error)

	// Ack acknowledges that messages with the given IDs
	Ack(ids ...string) error

	// Close gracefully closes any open connections to the broker.
	Close() error
}
