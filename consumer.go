package datanexus

import (
	"context"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

type consumer struct {
	interval     time.Duration
	done         chan struct{}
	logger       *logging.Logger
	broker       broker.Broker
	msgCh        chan<- *types.Metric
	consumeBatch int64
}

func newConsumer(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh chan<- *types.Metric, consumeBatch int64) *consumer {
	return &consumer{
		broker:       broker,
		interval:     interval,
		logger:       logger,
		done:         make(chan struct{}),
		msgCh:        msgCh,
		consumeBatch: consumeBatch,
	}
}

func (c *consumer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				c.logger.Info("Consumer done")
				return
			case <-ticker.C:
				metrics, err := c.broker.Consume(context.Background(), c.consumeBatch)
				if err != nil {
					c.logger.Error("could not consume messages")
					continue
				}
				var counter int
				for _, m := range metrics {
					c.msgCh <- m
					counter++
				}
				c.logger.Info("Successfulle consumed ", counter, " messages")
			}
		}
	}()
}

func (c *consumer) shutdown() {
	c.done <- struct{}{}
}
