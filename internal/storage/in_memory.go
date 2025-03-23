package storage

import (
	"sync"

	"github.com/haze518/data-nexus/internal/types"
)

// InMemoryStorage is a simple thread-safe in-memory implementation of the Storage interface.
// It stores metrics in a slice and supports insertion, draining, and length checking.
//
// This implementation is useful for staging metrics before exposing them via an HTTP handler
// (e.g., for Prometheus) or forwarding them to other components like brokers.
//
// Note: A buffer limit is not currently enforced (see TODO).
type InMemoryStorage struct {
	data map[string][]*types.Metric // Slice that holds all inserted metrics
	mu   sync.RWMutex               // Mutex to ensure thread-safe access
}

// NewInMemoryStorage returns a new instance of InMemoryStorage with an empty metric buffer.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[string][]*types.Metric),
		mu:   sync.RWMutex{},
	}
}

// Insert adds one or more metrics to the storage buffer.
// It locks the buffer to allow safe concurrent use.
func (s *InMemoryStorage) Insert(metrics ...*types.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range metrics {
		s.data[m.Name] = append(s.data[m.Name], m)
	}
}

// Drain returns all stored metrics and clears the buffer.
// The second return value is false if the buffer was already empty.
func (s *InMemoryStorage) Drain() (map[string][]*types.Metric, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return nil, false
	}

	vals := s.data
	s.data = make(map[string][]*types.Metric)

	return vals, true
}

// Len returns the current number of stored metrics.
func (s *InMemoryStorage) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, group := range s.data {
		count += len(group)
	}
	return count
}
