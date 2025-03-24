package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/haze518/data-nexus/pkg"
	"github.com/haze518/data-nexus/pkg/config"
)

func main() {
	cfg := config.NewConfig(
		config.WithGrpcAddr("0.0.0.0:50051"),
		config.WithHttpAddr("0.0.0.0:8080"),
		config.WithRedisConfig(config.RedisConfig{
			Addr:          "localhost:6379",
			Password:      "",
			DB:            1,
			PoolSize:      10,
			StreamName:    "test_stream",
			ConsumerGroup: "test_group",
			ConsumerID:    "test_consumer",
		}),
	)
	srv, err := datanexus.NewServer(&cfg)
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		srv.Start()
	}()

	<-sigChan
	srv.Shutdown()
}
