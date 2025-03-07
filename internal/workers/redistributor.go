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

type Redistributor struct {
	interval time.Duration
	done     chan struct{}
	logger   *logging.Logger
	broker   broker.Broker
	msgCh    chan<- *types.Metric
}

func NewRedistributor(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh chan<- *types.Metric) *Redistributor {
	return &Redistributor{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
		msgCh:    msgCh,
	}
}

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

func (r *Redistributor) Shutdown() {
	r.done <- struct{}{}
}

func (r *Redistributor) exec() error {
	servers, err := r.listInactiveServers()
	if err != nil {
		return fmt.Errorf("r.listInactiveServers: %w", err)
	}
	if len(servers) == 0 {
		return nil
	}
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})

	for _, srv := range servers {
		metrics, err := r.broker.MoveInactiveServerMsgs(srv, 50)
		if err != nil {
			return fmt.Errorf("broker.MoveInactiveServerMsgs: %w", err)
		}
		if len(metrics) > 0 {
			for _, m := range metrics {
				r.msgCh <- m
			}
			r.logger.Info(fmt.Sprintf("Successfully moved %d messages from server %s", len(metrics), srv))
			return nil
		}
	}
	return nil
}

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
