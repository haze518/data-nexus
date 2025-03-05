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
	msgCh    <-chan []*types.Metric
	storage  storage.Storage
}

func NewSinker(broker broker.Broker, interval time.Duration, logger *logging.Logger, msgCh <-chan []*types.Metric, storage storage.Storage) *Sinker {
	return &Sinker{
		broker:   broker,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
		msgCh:    msgCh,
		storage:  storage,
	}
}

func (s *Sinker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-s.done:
				s.logger.Info("DataSinker done")
				return
			case val := <-s.msgCh:
				if len(val) > 0 {
					s.storage.Insert(val...)
				}
			}
		}
	}()
}

func (s *Sinker) Shutdown() {
	close(s.done)
}
