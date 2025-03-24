package storage

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

type Opt func(st *InMemoryStorage)

func WithBucketDuration(val time.Duration) Opt {
	return func(st *InMemoryStorage) {
		st.bucketDuration = val
	}
}

// InMemoryStorage is a simple thread-safe in-memory implementation of the Storage interface.
// It stores metrics in a slice and supports insertion, draining, and length checking.
//
// This implementation is useful for staging metrics before exposing them via an HTTP handler
// (e.g., for Prometheus) or forwarding them to other components like brokers.
//
// Note: A buffer limit is not currently enforced (see TODO).
type InMemoryStorage struct {
	mu             sync.RWMutex
	buckets        map[time.Time]*bucket
	bucketDuration time.Duration
}

// NewInMemoryStorage returns a new instance of InMemoryStorage with an empty metric buffer.
func NewInMemoryStorage(opts ...Opt) *InMemoryStorage {
	st := &InMemoryStorage{
		mu:             sync.RWMutex{},
		buckets:        make(map[time.Time]*bucket),
		bucketDuration: 300 * time.Second,
	}

	for _, opt := range opts {
		opt(st)
	}

	return st
}

// Insert adds one or more metrics to the storage buffer.
// It locks the buffer to allow safe concurrent use.
func (s *InMemoryStorage) Insert(metrics ...*types.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range metrics {
		start := m.Timestamp.Truncate(s.bucketDuration)
		b, exists := s.buckets[start]
		if !exists {
			b = &bucket{
				startTime: start,
				metrics:   make(map[string][]*types.Metric),
			}
			s.buckets[start] = b
		}
		b.metrics[m.Name] = append(b.metrics[m.Name], m)
	}
}

// Drain returns all stored metrics and clears the buffer.
// The second return value is false if the buffer was already empty.
func (s *InMemoryStorage) Drain() (map[string][]*types.Metric, bool) {
	if s.Len() == 0 {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string][]*types.Metric, len(s.buckets))
	for _, bucket := range s.buckets {
		for name, m := range bucket.metrics {
			result[name] = append(result[name], m...)
		}
	}
	s.buckets = make(map[time.Time]*bucket)

	return result, true
}

// Len returns the current number of stored metrics.
func (s *InMemoryStorage) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int
	for _, bucket := range s.buckets {
		count += bucket.len()
	}
	return count
}

func (s *InMemoryStorage) RemoveOlderThan(interval time.Duration) []*types.Metric {
	s.mu.Lock()
	defer s.mu.Unlock()

	var count int
	var toRemove []*bucket
	now := time.Now()
	for start, bucket := range s.buckets {
		if now.Add(-interval).After(start) {
			toRemove = append(toRemove, bucket)
			count += bucket.len()
			delete(s.buckets, start)
		}
	}

	result := make([]*types.Metric, 0, count)
	for _, bucket := range toRemove {
		for _, m := range bucket.metrics {
			result = append(result, m...)
		}
	}

	return result
}

type bucket struct {
	startTime time.Time
	metrics   map[string][]*types.Metric
}

func (b *bucket) len() int {
	var count int
	for _, m := range b.metrics {
		count += len(m)
	}
	return count
}
