package datanexus

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/testutil"
	"github.com/haze518/data-nexus/internal/types"
	pb "github.com/haze518/data-nexus/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDataNexus(t *testing.T) {
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

	time.Sleep(1 * time.Second)

	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("failed to dial gRPC server: %v", err)
	}
	defer conn.Close()
	grpcClient := pb.NewMetricsServiceClient(conn)

	metricReq := &pb.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Type:      "gauge",
		Timestamp: time.Now().Unix(),
		Labels:    map[string]string{"service": "test"},
	}
	resp, err := grpcClient.IngestMetric(ctx, metricReq)
	if err != nil {
		t.Fatalf("IngestMetric failed: %v", err)
	}
	t.Logf("IngestMetric response: %v", resp)

	time.Sleep(2 * time.Second)

	httpResp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		t.Fatalf("failed to GET /metrics: %v", err)
	}
	defer httpResp.Body.Close()
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		t.Fatalf("failed to read HTTP response: %v", err)
	}
	output := string(body)
	t.Logf("HTTP /metrics output:\n%s", output)

	if !strings.Contains(output, "cpu_usage") {
		t.Errorf("expected output to contain 'cpu_usage', got: %s", output)
	}

	srv.Shutdown()
}

func TestDataNexus_ReassignMessages(t *testing.T) {
	client := testutil.SetupRedis(t)
	defer testutil.CleanupRedis(t, client)

	deadConfig := testutil.Config()
	deadConfig.GRPCAddr = "127.0.0.1:50051"
	deadConfig.RedisConfig.ConsumerID = "dead_server"
	deadServer, err := NewServer(&deadConfig)
	if err != nil {
		t.Fatalf("failed to create dead server: %v", err)
	}

	activeConfig := testutil.Config()
	activeConfig.GRPCAddr = "127.0.0.1:50052"
	activeConfig.RedisConfig.ConsumerID = "active_server"
	activeServer, err := NewServer(&activeConfig)
	if err != nil {
		t.Fatalf("failed to create active server: %v", err)
	}

	err = deadServer.broker.SetServerState(types.ServerStateInactive, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to set dead server state: %v", err)
	}

	for i := 0; i < 3; i++ {
		m, _ := proto.Marshal(&pb.Metric{
			Name:      "cpu_usage",
			Value:     42.5 + float64(i),
			Timestamp: time.Now().Unix(),
			Labels:    map[string]string{"service": "test"},
		})
		_, err = deadServer.broker.Publish(context.Background(), m)
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}
	}

	_, err = deadServer.broker.Consume(3)
	if err != nil {
		t.Fatalf("failed to consume messages from dead server: %v", err)
	}

	go func() {
		if err := activeServer.Start(); err != nil {
			t.Fatalf("server.Start() failed: %v", err)
		}
	}()
	time.Sleep(2 * time.Second)

	httpResp, err := http.Get("http://" + activeServer.httpSrv.Addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to GET /metrics: %v", err)
	}
	defer httpResp.Body.Close()
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		t.Fatalf("failed to read HTTP response: %v", err)
	}
	output := string(body)
	t.Logf("HTTP /metrics output:\n%s", output)

	if strings.Count(output, "cpu_usage") != 3 {
		t.Errorf("expected 3 occurrences of 'cpu_usage', but got %d", strings.Count(output, "cpu_usage"))
	}

	activeServer.Shutdown()
}
