package storage

import (
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/types"
)

func TestInsertAndLen(t *testing.T) {
	storage := NewInMemoryStorage()

	now := time.Now()
	metrics := []*types.Metric{
		{Name: "cpu", Value: 0.5, Timestamp: now},
		{Name: "cpu", Value: 0.6, Timestamp: now},
		{Name: "mem", Value: 0.9, Timestamp: now},
	}

	storage.Insert(metrics...)

	got := storage.Len()
	want := 3
	if got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
}

func TestDrain(t *testing.T) {
	storage := NewInMemoryStorage()

	now := time.Now()
	storage.Insert(
		&types.Metric{Name: "cpu", Value: 0.5, Timestamp: now},
		&types.Metric{Name: "mem", Value: 0.9, Timestamp: now},
	)

	result, ok := storage.Drain()
	if !ok {
		t.Fatalf("Drain() returned ok = false, want true")
	}

	if len(result["cpu"]) != 1 {
		t.Errorf("Expected 1 cpu metric, got %d", len(result["cpu"]))
	}
	if len(result["mem"]) != 1 {
		t.Errorf("Expected 1 mem metric, got %d", len(result["mem"]))
	}

	if storage.Len() != 0 {
		t.Errorf("Expected storage to be empty after drain, got %d metrics", storage.Len())
	}
}

func TestRemoveOlderThan(t *testing.T) {
	storage := NewInMemoryStorage()

	now := time.Now()
	old := now.Add(-10 * time.Minute)
	recent := now.Add(-1 * time.Minute)

	storage.Insert(
		&types.Metric{Name: "cpu", Value: 0.5, Timestamp: old},
		&types.Metric{Name: "mem", Value: 0.9, Timestamp: recent},
	)

	removed := storage.RemoveOlderThan(5 * time.Minute)

	if len(removed) != 1 {
		t.Fatalf("Expected 1 removed metric, got %d", len(removed))
	}
	if removed[0].Name != "cpu" {
		t.Errorf("Expected removed metric to be 'cpu', got '%s'", removed[0].Name)
	}

	if storage.Len() != 1 {
		t.Errorf("Expected 1 metric remaining in storage, got %d", storage.Len())
	}
}
