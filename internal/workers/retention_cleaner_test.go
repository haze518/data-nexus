package workers

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
)

func TestRetentionCleaner(t *testing.T) {
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	rs := newRedis("retention_srv", logger)

	st := storage.NewInMemoryStorage(storage.WithBucketDuration(1 * time.Second))

	now := time.Now()
	oldTime := now.Add(-10 * time.Second)

	st.Insert(
		&types.Metric{
			Name:      "cpu_usage",
			Value:     42.5,
			Timestamp: oldTime,
			ID:        stringPtr("id1"),
		},
		&types.Metric{
			Name:      "cpu_usage",
			Value:     43.0,
			Timestamp: oldTime,
			ID:        stringPtr("id2"),
		},
		&types.Metric{
			Name:      "cpu_usage",
			Value:     44.0,
			Timestamp: now,
			ID:        stringPtr("id3"),
		},
	)

	var wg sync.WaitGroup
	cleaner := NewRetentionCleaner(rs, 100*time.Millisecond, logger, st, 5*time.Second)
	cleaner.Start(&wg)

	time.Sleep(300 * time.Millisecond)

	cleaner.Shutdown()
	wg.Wait()

	if st.Len() != 1 {
		t.Fatalf("expected 1 metric after retention cleanup, got %d", st.Len())
	}
}

func stringPtr(s string) *string {
	return &s
}
