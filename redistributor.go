package datanexus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

type redistributor struct {
	interval time.Duration
	done     chan struct{}
	logger   *logging.Logger
	broker   broker.Broker
}

func newRedistributor(broker broker.Broker, interval time.Duration, logger *logging.Logger) *redistributor {
	return &redistributor{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

func (r *redistributor) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				servers, err := r.listInactiveServers()
				if err != nil {
					r.logger.Error(fmt.Sprintf("Failed to list inactive servers: %v", err))
					continue
				}
				if len(servers) == 0 {
					continue
				}
				rand.Shuffle(len(servers), func(i, j int) {
					servers[i], servers[j] = servers[j], servers[i]
				})

				for _, srv := range servers {
					messageIDs, err := r.broker.MoveInactiveServerMsgs(context.Background(), srv, 50)
					if err != nil {
						r.logger.Error(fmt.Sprintf("Failed to move messages for server %s: %v", srv, err))
						continue
					}
					if len(messageIDs) > 0 {
						r.logger.Info(fmt.Sprintf("Successfully moved %d messages from server %s", len(messageIDs), srv))
						continue
					}
				}
			}
		}
	}()
}

func (r *redistributor) listInactiveServers() ([]string, error) {
	servers, err := r.broker.ListServers(context.Background())
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
