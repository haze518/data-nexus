package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/storage"
)

type RetentionCleaner struct {
	interval          time.Duration
	done              chan struct{}
	logger            *logging.Logger
	broker            broker.Broker
	storage           storage.Storage
	retentionInterval time.Duration
}

func NewRetentionCleaner(broker broker.Broker, interval time.Duration, logger *logging.Logger, storage storage.Storage, retentionInterval time.Duration) *RetentionCleaner {
	return &RetentionCleaner{
		broker:            broker,
		interval:          interval,
		logger:            logger,
		storage:           storage,
		done:              make(chan struct{}),
		retentionInterval: retentionInterval,
	}
}

func (r *RetentionCleaner) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.done:
				r.logger.Info("RetentionCleaner done")
				return

			case <-ticker.C:
				deleted := r.storage.RemoveOlderThan(r.retentionInterval)
				ids := make([]string, 0, len(deleted))
				for _, m := range deleted {
					if m.ID != nil {
						ids = append(ids, *m.ID)
					}
				}
				if len(ids) > 0 {
					err := r.broker.Ack(ids...)
					if err != nil {
						r.logger.Error("broker.AckCollected: ", err)
					}
				}
			}
		}
	}()
}

func (r *RetentionCleaner) Shutdown() {
	close(r.done)
}
