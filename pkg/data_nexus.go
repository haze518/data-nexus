package datanexus

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/grpcserver"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/metrics"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/haze518/data-nexus/internal/workers"
	"github.com/haze518/data-nexus/pkg/config"
)

// Server is the main application struct that initializes and manages
// all internal components, including gRPC and HTTP servers, background workers,
// broker connection, and in-memory storage.
type Server struct {
	logger *logging.Logger
	broker broker.Broker

	grpcServer *grpcserver.Server
	httpSrv    *http.Server

	wg            sync.WaitGroup
	consumer      *workers.Consumer
	dataSinker    *workers.Sinker
	heartbeater   *workers.Heartbeater
	redistributor *workers.Redistributor
	acker         *workers.Acker
	cleaner       *workers.RetentionCleaner
}

// NewServer creates and initializes a new Server instance.
// It configures internal services and background workers based on the provided configuration.
func NewServer(config *config.Config) (*Server, error) {
	logger := logging.NewLogger(config.Logging.Level, config.Logging.Output)

	broker, err := broker.NewRedisBroker(config.RedisConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("broker.NewRedisBroker: %w", err)
	}

	grpcSrv := grpcserver.NewServer(config.GRPCAddr, broker)
	storage := storage.NewInMemoryStorage()

	collectedIDsCh := make(chan []string)
	metricsCh := make(chan []*types.Metric)

	consumer := workers.NewConsumer(broker, config.Worker.ConsumerInterval, logger, metricsCh, int64(config.Worker.BatchSize))
	sinker := workers.NewSinker(broker, config.Worker.SinkerInterval, logger, metricsCh, storage)
	heartbeater := workers.NewHeartbeater(broker, config.Worker.HeartbeatInterval, logger)
	redistributor := workers.NewRedistributor(broker, config.Worker.RedistributorInterval, logger, metricsCh)
	acker := workers.NewAcker(broker, config.Worker.AckerInterval, logger, collectedIDsCh)
	cleaner := workers.NewRetentionCleaner(broker, config.Worker.RetentionCleanerInterval, logger, storage, 15*time.Minute)

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
		cleaner:       cleaner,
		httpSrv: &http.Server{
			Addr:    config.HTTPAddr,
			Handler: mux,
		},
	}, nil
}

// Start launches all internal components of the server, including
// the gRPC server, HTTP metrics endpoint, and background workers.
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

	s.heartbeater.Start(&s.wg)
	s.consumer.Start(&s.wg)
	s.dataSinker.Start(&s.wg)
	s.redistributor.Start(&s.wg)
	s.acker.Start(&s.wg)
	s.cleaner.Start(&s.wg)

	return nil
}

// Shutdown gracefully stops all services and workers,
// waits for them to finish, and closes the broker connection.
func (s *Server) Shutdown() {
	s.grpcServer.Stop()
	s.httpSrv.Shutdown(context.Background())

	s.redistributor.Shutdown()
	s.heartbeater.Shutdown()
	s.consumer.Shutdown()
	s.dataSinker.Shutdown()
	s.acker.Shutdown()
	s.cleaner.Shutdown()

	s.wg.Wait()
	s.broker.Close()
}
