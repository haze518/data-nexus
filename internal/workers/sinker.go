package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
)

// Sinker is a background worker responsible for writing incoming metrics
// from a channel to a persistent storage. It acts as the final stage
// in the metrics processing pipeline.
type Sinker struct {
	interval time.Duration          // (Reserved) Interval for future use; currently unused
	done     chan struct{}          // Channel used to signal shutdown
	logger   *logging.Logger        // Logger for logging sink activity
	broker   broker.Broker          // Broker (currently unused in Sinker, may be used for future enhancements)
	msgCh    <-chan []*types.Metric // Channel receiving batches of metrics to persist
	storage  storage.Storage        // Storage interface used to persist metrics
}

// NewSinker returns a new Sinker that writes metric batches received
// from msgCh into the given storage.
func NewSinker(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh <-chan []*types.Metric, storage storage.Storage) *Sinker {
	return &Sinker{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
		msgCh:    msgCh,
		storage:  storage,
	}
}

// Start launches the Sinker in a separate goroutine.
// It listens on the msgCh for metric batches and writes them to storage.
func (s *Sinker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-s.done:
				s.logger.Info("DataSinker done")
				return

			case val := <-s.msgCh:
				if len(val) > 0 {
					s.storage.Insert(val...)
				}
			}
		}
	}()
}

// Shutdown signals the Sinker to stop by closing the done channel.
// The goroutine will exit gracefully after processing is complete.
func (s *Sinker) Shutdown() {
	close(s.done)
}
