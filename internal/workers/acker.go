package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
)

// Acker periodically acknowledges collected metric IDs using a broker.
// It listens on a channel for batches of collected IDs and calls the
// broker's AckCollected method to confirm successful processing.
type Acker struct {
	interval       time.Duration   // Time interval for processing acknowledgements
	done           chan struct{}   // Channel to signal shutdown
	logger         *logging.Logger // Logger for info and error messages
	broker         broker.Broker   // Broker used to acknowledge collected metrics
	collectedIDsCh <-chan []string // Channel of collected metric ID batches to acknowledge
}

// NewAcker returns a new Acker instance that listens on collectedIDsCh
// and periodically acknowledges collected metric IDs via the provided broker.
func NewAcker(broker broker.Broker, interval time.Duration, logger *logging.Logger, collectedIDsCh <-chan []string) *Acker {
	return &Acker{
		broker:         broker,
		interval:       interval,
		logger:         logger,
		done:           make(chan struct{}),
		collectedIDsCh: collectedIDsCh,
	}
}

// Start begins the background goroutine that listens for collected metric IDs
// and sends acknowledgements using the broker. It adds itself to the provided
// WaitGroup and marks completion on exit.
func (a *Acker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(a.interval)
		defer ticker.Stop()

		for {
			select {
			case <-a.done:
				a.logger.Info("Acker done")
				return

			case ids := <-a.collectedIDsCh:
				err := a.broker.AckCollected(ids...)
				if err != nil {
					a.logger.Error("unable to ack collected metrics", ids)
				}
			}
		}
	}()
}

// Shutdown signals the Acker to stop by closing the done channel.
// It allows the Start loop to exit gracefully.
func (a *Acker) Shutdown() {
	close(a.done)
}
