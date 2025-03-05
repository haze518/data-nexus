package storage

import (
	"sync"

	"github.com/haze518/data-nexus/internal/types"
)

type InMemoryStorage struct {
	data []*types.Metric
	mu sync.Mutex
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make([]*types.Metric, 0),
		mu: sync.Mutex{},
	}
}

func (s *InMemoryStorage) Insert(val ...*types.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, val...)
}

func (s *InMemoryStorage) Drain() ([]*types.Metric, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return nil, false
	}

	vals := s.data
	s.data = nil

	return vals, true
}

func (s *InMemoryStorage) Len() int {
	return len(s.data)
}
