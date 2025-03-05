package datanexus

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal"
	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/grpcserver"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/metrics"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
)

type Server struct {
	logger *logging.Logger

	broker broker.Broker

	grpcServer *grpcserver.Server
	httpSrv    *http.Server

	wg            sync.WaitGroup
	consumer      *consumer
	dataSinker    *dataSinker
	heartbeater   *heartbeater
	redistributor *redistributor
	acker         *acker
}

type Config struct {
	GRPCAddr    string
	HTTPAddr    string
	RedisConfig internal.RedisConfig
}

func NewServer(config *Config) (*Server, error) {
	logger := logging.NewLogger(logging.InfoLevel, os.Stdout)
	broker, err := broker.NewRedisBroker(context.Background(), config.RedisConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("broker.NewRedisBroker: %w", err)
	}

	grpcSrv := grpcserver.NewServer(config.GRPCAddr, broker)

	storage := storage.NewInMemoryStorage()
	collectedIDsCh := make(chan []string)
	metricsCh := make(chan *types.Metric, 1)
	interval := 1 * time.Second
	batchLen := 1

	consumer := newConsumer(broker, interval, logger, metricsCh, int64(batchLen))
	sinker := newDataSinker(broker, interval, logger, metricsCh, storage, batchLen)
	heartbeater := newHeartbeater(broker, interval, logger)
	redistributor := newRedistributor(broker, interval, logger, metricsCh)
	acker := newAcker(broker, interval, logger, collectedIDsCh)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(storage, collectedIDsCh))

	return &Server{
		logger:        logger,
		broker:        broker,
		grpcServer:    grpcSrv,
		consumer:      consumer,
		dataSinker:    sinker,
		heartbeater:   heartbeater,
		redistributor: redistributor,
		acker:         acker,
		httpSrv: &http.Server{
			Addr: config.HTTPAddr,
			Handler: mux,
		},
	}, nil
}

func (s *Server) Start() error {
	go func() {
		if err := s.grpcServer.Start(); err != nil {
			s.logger.Error("Failed to start grpc server")
			return
		}
	}()
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error(fmt.Sprintf("Failed to start HTTP server: %v", err))
		}
	}()

	s.heartbeater.start(&s.wg)
	s.consumer.start(&s.wg)
	s.dataSinker.start(&s.wg)
	s.redistributor.start(&s.wg)
	s.acker.start(&s.wg)
	return nil
}

func (s *Server) Shutdown() {
	s.grpcServer.Stop()
	s.httpSrv.Shutdown(context.Background())

	s.redistributor.shutdown()
	s.heartbeater.shutdown()
	s.consumer.shutdown()
	s.dataSinker.shutdown()
	s.acker.shutdown()

	s.wg.Wait()
	s.broker.Close()
}
