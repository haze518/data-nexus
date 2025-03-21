package types

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/protobuf/proto"
)

// object is a reusable byte buffer wrapper used for protobuf unmarshalling.
// It is managed by a sync.Pool to reduce allocation pressure.
type object struct {
	Data []byte
}

// reset clears the buffer content while retaining capacity.
func (o *object) reset() {
	o.Data = o.Data[:0]
}

// pool is a global buffer pool used to reuse byte slices during unmarshalling.
// Each object in the pool holds a reusable byte slice.
var pool sync.Pool = sync.Pool{
	New: func() any {
		return &object{
			Data: make([]byte, 0, 1024),
		}
	},
}

// Metric represents a single metric data point collected from the system.
// It includes standard Prometheus-compatible fields such as name, value,
// type, timestamp, and optional key-value labels.
//
// The optional ID field is used to track the message ID from the broker (e.g., Redis stream).
type Metric struct {
	Name      string            // Metric name (e.g., "http_requests_total")
	Value     float64           // Metric value
	Type      string            // Metric type (e.g., "counter", "gauge")
	Timestamp time.Time         // Time at which the metric was collected
	Labels    map[string]string // Optional labels (e.g., method="GET", status="200")
	ID        *string           // Optional broker-assigned message ID
}

// Marshal encodes a Metric into its protobuf representation using proto.Marshal.
// Returns the encoded byte slice or an error.
func Marshal(val *Metric) ([]byte, error) {
	if val == nil {
		return nil, fmt.Errorf("marshal, val is nil")
	}
	return proto.Marshal(&pb.Metric{
		Name:      val.Name,
		Value:     val.Value,
		Type:      val.Type,
		Timestamp: val.Timestamp.Unix(),
		Labels:    val.Labels,
	})
}

// Unmarshal decodes a protobuf-encoded metric from a string into a Metric struct.
// It uses an internal object pool to minimize allocations.
//
// Returns the parsed Metric or an error if decoding fails.
func Unmarshal(data string) (*Metric, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("received an empty data on unmarshal")
	}

	buf := pool.Get().(*object)
	defer func() {
		buf.reset()
		pool.Put(buf)
	}()
	buf.Data = append(buf.Data, data...)

	var pbmetric pb.Metric
	if err := proto.Unmarshal(buf.Data, &pbmetric); err != nil {
		return nil, fmt.Errorf("proto.Unmarshal: %w", err)
	}

	return &Metric{
		Name:      pbmetric.Name,
		Value:     pbmetric.Value,
		Type:      pbmetric.Type,
		Timestamp: time.Unix(pbmetric.Timestamp, 0),
		Labels:    pbmetric.Labels,
	}, nil
}
