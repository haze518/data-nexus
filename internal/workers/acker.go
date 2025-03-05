package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
)

type Acker struct {
	interval       time.Duration
	done           chan struct{}
	logger         *logging.Logger
	broker         broker.Broker
	collectedIDsCh <-chan []string
}

func NewAcker(broker broker.Broker, interval time.Duration, logger *logging.Logger, collectedIDsCh <-chan []string) *Acker {
	return &Acker{
		broker:         broker,
		interval:       interval,
		logger:         logger,
		done:           make(chan struct{}),
		collectedIDsCh: collectedIDsCh,
	}
}

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

func (a *Acker) Shutdown() {
	close(a.done)
}
