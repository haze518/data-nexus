package workers

import (
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
)

type Sinker struct {
	interval time.Duration
	done     chan struct{}
	logger   *logging.Logger
	broker   broker.Broker
	msgCh    <-chan *types.Metric
	storage  storage.Storage
	batchLen int
}

func NewSinker(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh <-chan *types.Metric, storage storage.Storage, batchLen int) *Sinker {
	return &Sinker{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
		msgCh:    msgCh,
		storage:  storage,
		batchLen: batchLen,
	}
}

func (s *Sinker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		// todo replace ticker to timer
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		batch := make([]*types.Metric, 0, s.batchLen)
		for {
			select {
			case <-s.done:
				s.logger.Info("DataSinker done")
				return
			case <-ticker.C:
				select {
				case val := <-s.msgCh:
					batch = append(batch, val)
					if len(batch) == s.batchLen {
						s.storage.Insert(batch...)
						batch = batch[:0]
					}
				default:
				}
			}
		}
	}()
}

func (s *Sinker) Shutdown() {
	s.done <- struct{}{}
}
