package types

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	pb "github.com/haze518/data-nexus/proto"
)

type Metric struct {
	Name      string
	Value     float64
	Timestamp time.Time
	Labels    map[string]string
}

func Marshal(val *Metric) ([]byte, error) {
	if val == nil {
		return nil, fmt.Errorf("marshal, val is nil")
	}
	return proto.Marshal(&pb.Metric{
		Name:      val.Name,
		Value:     val.Value,
		Timestamp: val.Timestamp.Unix(),
		Labels:    val.Labels,
	})
}

func Unmarshal(data []byte) (*Metric, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("received an empty data on unmarshal")
	}
	var pbmetric pb.Metric
	if err := proto.Unmarshal(data, &pbmetric); err != nil {
		return nil, fmt.Errorf("proto.Unmarshal: %w", err)
	}
	return &Metric{
		Name:      pbmetric.Name,
		Value:     pbmetric.Value,
		Timestamp: time.Unix(pbmetric.Timestamp, 0),
		Labels:    pbmetric.Labels,
	}, nil
}
