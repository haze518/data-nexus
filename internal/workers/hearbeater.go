package workers

import (
	"fmt"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

// Heartbeater is responsible for periodically updating the server state
// to "active" in the broker. When shut down, it sets the server state
// to "inactive" to indicate that the service is stopping.
type Heartbeater struct {
	interval time.Duration   // Interval at which heartbeats are sent
	done     chan struct{}   // Channel used to signal shutdown
	logger   *logging.Logger // Logger for error and status reporting
	broker   broker.Broker   // Broker used to update the server state
}

// NewHeartbeater creates a new Heartbeater that updates the server state
// via the provided broker at the given interval.
func NewHeartbeater(broker broker.Broker, interval time.Duration, logger *logging.Logger) *Heartbeater {
	return &Heartbeater{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// Start launches the heartbeater in a separate goroutine.
// It periodically updates the server state to "active".
// On shutdown, it sets the server state to "inactive".
func (h *Heartbeater) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for {
			select {
			case <-h.done:
				// Set server state to inactive on shutdown
				err := h.broker.SetServerState(types.ServerStateInactive, 60*time.Second)
				if err != nil {
					h.logger.Error(fmt.Sprintf("Failed to SetServerState: %v", err))
				}
				h.logger.Info("Heartbeater done")
				return

			case <-ticker.C:
				// Set server state to active periodically
				err := h.broker.SetServerState(types.ServerStateActive, 60*time.Second)
				if err != nil {
					h.logger.Error(fmt.Sprintf("Failed to SetServerState: %v", err))
				}
			}
		}
	}()
}

// Shutdown stops the Heartbeater by closing the done channel.
// This triggers a state change to "inactive" before exiting.
func (h *Heartbeater) Shutdown() {
	close(h.done)
}
