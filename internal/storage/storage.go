package storage

import (
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

// Storage defines an interface for a temporary buffer
// used to collect, access, and drain metrics.
type Storage interface {
	// Insert adds one or more metrics to the buffer.
	Insert(val ...*types.Metric)

	// Drain returns all currently stored metrics and clears the buffer.
	// The second return value is true if there were any metrics to return, false otherwise.
	Drain() (map[string][]*types.Metric, bool)

	// Len returns the current number of metrics stored in the buffer.
	Len() int

	// RemoveOlderThan removes all metric buckets whose start time is older than the given interval
	// from the current time. It returns a flat slice of removed metrics.
	RemoveOlderThan(interval time.Duration) []*types.Metric
}
