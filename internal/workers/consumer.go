package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

type Consumer struct {
	interval     time.Duration
	done         chan struct{}
	logger       *logging.Logger
	broker       broker.Broker
	msgCh        chan<- []*types.Metric
	consumeBatch int64
}

func NewConsumer(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh chan<- []*types.Metric, consumeBatch int64) *Consumer {
	return &Consumer{
		broker:       broker,
		interval:     interval,
		logger:       logger,
		done:         make(chan struct{}),
		msgCh:        msgCh,
		consumeBatch: consumeBatch,
	}
}

func (c *Consumer) Start(wg *sync.WaitGroup) {
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
				metrics, err := c.broker.Consume(c.consumeBatch)
				if err != nil {
					c.logger.Error("could not consume messages", err.Error())
					continue
				}
				if len(metrics) > 0 {
					c.msgCh <- metrics
					c.logger.Info("Successfulle consumed ", len(metrics), " messages")
				}
			}
		}
	}()
}

func (c *Consumer) Shutdown() {
	close(c.done)
}
