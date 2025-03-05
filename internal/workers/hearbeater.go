package workers

import (
	"fmt"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

type Heartbeater struct {
	interval time.Duration
	done     chan struct{}
	logger   *logging.Logger
	broker   broker.Broker
}

func NewHeartbeater(broker broker.Broker, interval time.Duration, logger *logging.Logger) *Heartbeater {
	return &Heartbeater{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

func (h *Heartbeater) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		for {
			select {
			case <-h.done:
				err := h.broker.SetServerState(types.ServerStateInactive, 60*time.Second)
				if err != nil {
					h.logger.Error(fmt.Sprintf("Failed to SetServerState: %v", err))
				}
				h.logger.Info("Heartbeater done")
				return
			case <-ticker.C:
				err := h.broker.SetServerState(types.ServerStateActive, 60*time.Second)
				if err != nil {
					h.logger.Error(fmt.Sprintf("Failed to SetServerState: %v", err))
				}
			}
		}
	}()
}

func (h *Heartbeater) Shutdown() {
	close(h.done)
}
