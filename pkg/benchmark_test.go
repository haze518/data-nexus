package datanexus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/proto"
	"google.golang.org/grpc"
)

func BenchmarkE2E(t *testing.B) {
	ctx := context.Background()
	redisClient := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, redisClient)

	grpcAddr := "127.0.0.1:50051"
	httpAddr := "127.0.0.1:8080"

	config := testutil.Config()
	config.GRPCAddr = grpcAddr
	config.HTTPAddr = httpAddr

	srv, err := NewServer(&config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			t.Fatalf("server.Start() failed: %v", err)
		}
	}()
	defer srv.Shutdown()
	time.Sleep(500 * time.Millisecond)

	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("failed to dial gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient := proto.NewMetricsServiceClient(conn)

	numMetrics := 1000
	for i := 0; i < numMetrics; i++ {
		metricReq := &proto.Metric{
			Name:      fmt.Sprintf("cpu_usage_%d", i),
			Value:     float64(i),
			Type:      "gauge",
			Timestamp: time.Now().Unix(),
			Labels:    map[string]string{"service": "test"},
		}
		_, err = grpcClient.IngestMetric(ctx, metricReq)
		if err != nil {
			t.Fatalf("IngestMetric failed: %v", err)
		}
	}

	time.Sleep(1 * time.Second)

	var count int
	for count < numMetrics {
		httpResp, err := http.Get("http://" + httpAddr + "/metrics")
		if err != nil {
			t.Fatalf("failed to GET /metrics: %v", err)
		}
		body, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			t.Fatalf("failed to read HTTP response: %v", err)
		}
		count += strings.Count(string(body), "cpu_usage_")
	}
}

func BenchmarkE2EBatch(t *testing.B) {
	ctx := context.Background()

	redisClient := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, redisClient)

	grpcAddr := "127.0.0.1:50051"
	httpAddr := "127.0.0.1:8080"

	config := testutil.Config()
	config.GRPCAddr = grpcAddr
	config.HTTPAddr = httpAddr

	srv, err := NewServer(&config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			t.Fatalf("server.Start() failed: %v", err)
		}
	}()
	defer srv.Shutdown()
	time.Sleep(500 * time.Millisecond)

	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("failed to dial gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient := proto.NewMetricsServiceClient(conn)

	batchSize := 1000
	numBatches := 1000
	totalMetrics := batchSize * numBatches

	for batch := 0; batch < numBatches; batch++ {
		metrics := make([]*proto.Metric, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			metricReq := &proto.Metric{
				Name:      fmt.Sprintf("cpu_usage_%d", batch*batchSize+i),
				Value:     float64(i),
				Type:      "gauge",
				Timestamp: time.Now().Unix(),
				Labels:    map[string]string{"service": "test"},
			}
			metrics = append(metrics, metricReq)
		}
		_, err = grpcClient.IngestMetrics(ctx, &proto.BatchMetrics{Metrics: metrics})
		if err != nil {
			t.Fatalf("IngestMetrics failed: %v", err)
		}
	}

	time.Sleep(1 * time.Second)

	var count int
	for count < totalMetrics {
		httpResp, err := http.Get("http://" + httpAddr + "/metrics")
		if err != nil {
			t.Fatalf("failed to GET /metrics: %v", err)
		}
		body, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			t.Fatalf("failed to read HTTP response: %v", err)
		}
		count += strings.Count(string(body), "cpu_usage_")
	}

	if count < totalMetrics {
		t.Errorf("expected at least %d metrics, got %d", totalMetrics, count)
	}
}
