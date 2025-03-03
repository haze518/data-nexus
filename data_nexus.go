package datanexus

import (
	"net/http"
	"os"
	"sync"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/internal/grpcserver"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/metrics"
	"github.com/haze518/data-nexus/internal/storage"
)

type Server struct {
	logger *logging.Logger

	broker broker.Broker

	grpcServer *grpcserver.Server

	wg            sync.WaitGroup
	grpcSrv       *grpcserver.Server
	consumer      *consumer
	dataSinker    *dataSinker
	heartbeater   *heartbeater
	redistributor *redistributor
}

type Config struct {
	GRPCAddr string
}

func NewServer(config *Config) *Server {
	grpcSrv := grpcserver.NewServer(config.GRPCAddr)
	storage := storage.NewInMemoryStorage()
	collectorCh := make(chan []string)
	http.Handle("/metrics", metrics.Handler(storage, collectorCh))
	return &Server{
		logger: logging.NewLogger(logging.ErrorLevel, os.Stdout),
		grpcServer: grpcSrv,
	}
}

func (s *Server) Start() error {
	go func() {
		if err := s.grpcSrv.Start(); err != nil {
			s.logger.Error("Failed to start grpc server")
			return
		}
	}()

	s.heartbeater.start(&s.wg)
	s.consumer.start(&s.wg)
	s.dataSinker.start(&s.wg)
	s.redistributor.start(&s.wg)
	return nil
}

func (s *Server) Shutdown() {
	s.grpcServer.Stop()

	s.heartbeater.shutdown()
	s.consumer.shutdown()
	s.dataSinker.shutdown()
	s.redistributor.shutdown()

	s.wg.Wait()
	s.broker.Close()
}
