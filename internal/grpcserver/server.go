package grpcserver

import (
	"fmt"
	"net"

	"github.com/haze518/data-nexus/internal/broker"
	"github.com/haze518/data-nexus/proto"
	"google.golang.org/grpc"
)

// Server implements the gRPC MetricsService server.
// It handles metric ingestion and manages gRPC lifecycle.
type Server struct {
	proto.UnimplementedMetricsServiceServer               // Embeds unimplemented server for forward compatibility
	grpcServer                              *grpc.Server  // Underlying gRPC server
	broker                                  broker.Broker // Broker used to publish incoming metrics
	addr                                    string        // Address on which the gRPC server listens
}

// NewServer creates a new gRPC server that listens on the given address
// and uses the provided broker to handle metric publishing.
func NewServer(addr string, broker broker.Broker) *Server {
	return &Server{
		grpcServer: grpc.NewServer(),
		addr:       addr,
		broker:     broker,
	}
}

// Start launches the gRPC server and begins listening for incoming connections.
// It registers the MetricsService implementation with the gRPC runtime.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	proto.RegisterMetricsServiceServer(s.grpcServer, s)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully shuts down the gRPC server, allowing in-flight requests to complete.
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
