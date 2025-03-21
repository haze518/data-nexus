package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
)

// Consumer is a worker that periodically consumes batches of metrics from a broker.
// Retrieved metrics are pushed into a channel for further processing.
type Consumer struct {
	interval     time.Duration          // Polling interval for consuming messages
	done         chan struct{}          // Channel to signal shutdown
	logger       *logging.Logger        // Logger for informational and error output
	broker       broker.Broker          // Broker interface used to fetch messages
	msgCh        chan<- []*types.Metric // Channel to send consumed metric batches
	consumeBatch int64                  // Maximum number of messages to consume in one batch
}

// NewConsumer returns a new Consumer that fetches messages from the given broker,
// with the specified interval and batch size, and sends them to msgCh.
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

// Start launches the consumer in a separate goroutine.
// It periodically consumes messages from the broker and sends them to msgCh.
// This method also registers itself in the provided WaitGroup.
func (c *Consumer) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.done:
				// Shutdown signal received
				c.logger.Info("Consumer done")
				return

			case <-ticker.C:
				// Attempt to consume a batch of metrics
				metrics, err := c.broker.Consume(c.consumeBatch)
				if err != nil {
					c.logger.Error("could not consume messages", err.Error())
					continue
				}

				// Send metrics to the output channel if any were received
				if len(metrics) > 0 {
					c.msgCh <- metrics
					c.logger.Info("Successfully consumed ", len(metrics), " messages")
				}
			}
		}
	}()
}

// Shutdown signals the consumer to stop by closing the done channel.
// The Start loop will exit gracefully after receiving this signal.
func (c *Consumer) Shutdown() {
	close(c.done)
}
