package storage

import "github.com/haze518/data-nexus/internal/types"

// todo add redis id somewhere
type Storage interface {
	Insert(val ...*types.Metric)
	Drain() ([]*types.Metric, bool)
	Len() int
}
