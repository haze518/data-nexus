package grpcserver

import (
	"context"
	"fmt"
	"time"

	"github.com/haze518/data-nexus/internal/types"
	"github.com/haze518/data-nexus/proto"
)

func (s *Server) IngestMetric(ctx context.Context, metric *proto.Metric) (*proto.IngestResponse, error) {
	id, err := s.broker.Publish(ctx, &types.Metric{
		Name:      metric.Name,
		Value:     metric.Value,
		Type:      metric.Type,
		Timestamp: time.Unix(metric.Timestamp, 0),
		Labels:    metric.Labels,
	})
	if err != nil {
		return nil, fmt.Errorf("broker.Publish: %w", err)
	}
	return &proto.IngestResponse{EventId: id}, nil
}
