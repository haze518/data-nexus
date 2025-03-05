package grpcserver

import (
	"fmt"
	"net"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/proto"
	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedMetricsServiceServer
	grpcServer *grpc.Server
	broker     broker.Broker
	addr       string
}

func NewServer(addr string, broker broker.Broker) *Server {
	return &Server{
		grpcServer: grpc.NewServer(),
		addr:       addr,
		broker:     broker,
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	proto.RegisterMetricsServiceServer(s.grpcServer, s)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
