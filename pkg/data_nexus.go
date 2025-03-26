package datanexus

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/grpcserver"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/metrics"
	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/haze518/data-nexus/internal/workers"
	"github.com/haze518/data-nexus/pkg/config"
)

type Option func(*ServerOptions)

type ServerOptions struct {
	logger     *logging.Logger
	broker     broker.Broker
	storage    storage.Storage
	grpcServer *grpcserver.Server
}

func WithLogger(logger *logging.Logger) Option {
	return func(so *ServerOptions) {
		so.logger = logger
	}
}

func WithBroker(broker broker.Broker) Option {
	return func(so *ServerOptions) {
		so.broker = broker
	}
}

func WithStorage(storage storage.Storage) Option {
	return func(so *ServerOptions) {
		so.storage = storage
	}
}

func WithGrpcServer(srv *grpcserver.Server) Option {
	return func(so *ServerOptions) {
		so.grpcServer = srv
	}
}

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
func NewServer(config *config.Config, opts ...Option) (*Server, error) {
	var srvOpt ServerOptions
	var err error

	collectedIDsCh := make(chan []string)
	metricsCh := make(chan []*types.Metric)

	for _, o := range opts {
		o(&srvOpt)
	}

	if srvOpt.logger == nil {
		srvOpt.logger = logging.NewLogger(config.Logging.Level, config.Logging.Output)
	}
	if srvOpt.broker == nil {
		srvOpt.broker, err = broker.NewRedisBroker(config.RedisConfig, srvOpt.logger)
		if err != nil {
			return nil, fmt.Errorf("broker.NewRedisBroker: %w", err)
		}
	}
	if srvOpt.grpcServer == nil {
		srvOpt.grpcServer = grpcserver.NewServer(config.GRPCAddr, srvOpt.broker)
	}
	if srvOpt.storage == nil {
		srvOpt.storage = storage.NewInMemoryStorage()
	}

	consumer := workers.NewConsumer(
		srvOpt.broker,
		config.Worker.ConsumerInterval,
		srvOpt.logger,
		metricsCh,
		int64(config.Worker.BatchSize),
	)
	sinker := workers.NewSinker(
		srvOpt.broker, config.Worker.SinkerInterval, srvOpt.logger, metricsCh, srvOpt.storage,
	)
	heartbeater := workers.NewHeartbeater(srvOpt.broker, config.Worker.HeartbeatInterval, srvOpt.logger)
	redistributor := workers.NewRedistributor(
		srvOpt.broker,
		config.Worker.RedistributorInterval,
		srvOpt.logger,
		metricsCh,
	)
	acker := workers.NewAcker(srvOpt.broker, config.Worker.AckerInterval, srvOpt.logger, collectedIDsCh)
	cleaner := workers.NewRetentionCleaner(
		srvOpt.broker,
		config.Worker.RetentionCleanerInterval,
		srvOpt.logger,
		srvOpt.storage,
		config.Worker.RetentionMaxAge,
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(srvOpt.storage, collectedIDsCh))

	return &Server{
		logger:        srvOpt.logger,
		broker:        srvOpt.broker,
		grpcServer:    srvOpt.grpcServer,
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
			s.logger.Error("Failed to start grpc server", err)
			return
		}
	}()

	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Failed to start HTTP server: ", err)
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
