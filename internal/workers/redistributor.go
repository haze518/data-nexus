package workers

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

// Redistributor is a background worker that periodically checks for inactive servers,
// pulls undelivered metrics from them, and redistributes those messages for processing.
type Redistributor struct {
	interval time.Duration          // Interval between redistribution attempts
	done     chan struct{}          // Channel used to signal shutdown
	logger   *logging.Logger        // Logger for logging redistribution activity
	broker   broker.Broker          // Broker interface used to fetch and move messages
	msgCh    chan<- []*types.Metric // Channel to send redistributed metric batches
}

// NewRedistributor creates and returns a new Redistributor instance.
func NewRedistributor(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh chan<- []*types.Metric) *Redistributor {
	return &Redistributor{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
		msgCh:    msgCh,
	}
}

// Start launches the Redistributor in a background goroutine.
// It periodically attempts to fetch metrics from inactive servers
// and push them to the msgCh for reprocessing.
func (r *Redistributor) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		timer := time.NewTimer(r.interval)
		for {
			select {
			case <-r.done:
				timer.Stop()
				r.logger.Info("Redistributor done")
				return

			case <-timer.C:
				err := r.exec()
				if err != nil {
					r.logger.Error("r.exec", err.Error())
				}
				timer.Reset(r.interval)
			}
		}
	}()
}

// Shutdown gracefully stops the Redistributor by closing the done channel.
func (r *Redistributor) Shutdown() {
	close(r.done)
}

// exec is the core logic of the Redistributor. It identifies inactive servers,
// shuffles them randomly, and attempts to move messages from each until successful.
func (r *Redistributor) exec() error {
	servers, err := r.listInactiveServers()
	if err != nil {
		return fmt.Errorf("r.listInactiveServers: %w", err)
	}
	if len(servers) == 0 {
		return nil
	}

	// Randomize server order to balance load
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})

	for _, srv := range servers {
		metrics, err := r.broker.MoveInactiveServerMsgs(srv, 50)
		if err != nil {
			return fmt.Errorf("broker.MoveInactiveServerMsgs: %w", err)
		}
		if len(metrics) > 0 {
			r.msgCh <- metrics
			r.logger.Info(fmt.Sprintf("Successfully moved %d messages from server %s", len(metrics), srv))
			return nil
		}
	}
	return nil
}

// listInactiveServers queries the broker for all known servers,
// then filters and returns only those that are marked as inactive.
func (r *Redistributor) listInactiveServers() ([]string, error) {
	servers, err := r.broker.ListServers()
	if err != nil {
		return nil, fmt.Errorf("r.broker.ListServers: %w", err)
	}

	inactiveSrvs := make([]string, 0, len(servers))
	for srv, state := range servers {
		if state == types.ServerStateInactive {
			inactiveSrvs = append(inactiveSrvs, srv)
		}
	}
	return inactiveSrvs, nil
}
