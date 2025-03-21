package storage

import "github.com/haze518/data-nexus/internal/types"

// Storage defines an interface for a temporary buffer
// used to collect, access, and drain metrics.
type Storage interface {
	// Insert adds one or more metrics to the buffer.
	Insert(val ...*types.Metric)

	// Drain returns all currently stored metrics and clears the buffer.
	// The second return value is true if there were any metrics to return, false otherwise.
	Drain() ([]*types.Metric, bool)

	// Len returns the current number of metrics stored in the buffer.
	Len() int
}
