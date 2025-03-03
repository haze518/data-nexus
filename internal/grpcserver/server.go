package grpcserver

import (
	"context"
	"fmt"
	"net"

	"github.com/haze518/data-nexus/proto"
	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedMetricsServiceServer
	grpcServer *grpc.Server
	addr       string
}

func NewServer(addr string) *Server {
	return &Server{
		grpcServer: grpc.NewServer(),
		addr:       addr,
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

func (s *Server) IngestMetric(ctx context.Context, metric *proto.Metric) (*proto.IngestResponse, error) {
	return nil, nil
}
