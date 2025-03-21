package grpcserver

import (
	"context"
	"fmt"

	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/protobuf/proto"
)

// IngestMetrics handles batch ingestion of metric data via gRPC.
//
// It serializes each Metric in the incoming BatchMetrics request
// and publishes them to the broker in a single batch operation.
// On success, it returns a BatchIngestResponse containing a list of assigned event IDs.
// On failure, it returns an appropriate error.
func (s *Server) IngestMetrics(ctx context.Context, req *pb.BatchMetrics) (*pb.BatchIngestResponse, error) {
	vals := make([][]byte, 0, len(req.Metrics))
	for _, m := range req.Metrics {
		metric, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("proto.Marshal: %w", err)
		}
		vals = append(vals, metric)
	}

	ids, err := s.broker.PublishBatch(ctx, vals)
	if err != nil {
		return nil, fmt.Errorf("broker.PublishBatch: %w", err)
	}

	return &pb.BatchIngestResponse{EventIds: ids}, nil
}
