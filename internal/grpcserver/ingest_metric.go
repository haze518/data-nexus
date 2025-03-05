package grpcserver

import (
	"context"
	"fmt"

	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/protobuf/proto"
)

func (s *Server) IngestMetric(ctx context.Context, metric *pb.Metric) (*pb.IngestResponse, error) {
	val, err := proto.Marshal(metric)
	if err != nil {
		return nil, fmt.Errorf("pb.Marshal: %w", err)
	}
	id, err := s.broker.Publish(ctx, val)
	if err != nil {
		return nil, fmt.Errorf("broker.Publish: %w", err)
	}
	return &pb.IngestResponse{EventId: id}, nil
}
