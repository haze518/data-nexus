package testutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal"
	"github.com/redis/go-redis/v9"
)

var once sync.Once
var testRedisConfig internal.RedisConfig

func TestRedisConfig() internal.RedisConfig {
	once.Do(func() {
		testRedisConfig = internal.RedisConfig{
			Addr:          "localhost:6379",
			Password:      "",
			DB:            1,
			PoolSize:      10,
			StreamName:    "test_stream",
			ConsumerGroup: "test_group",
			ConsumerID:    "test_consumer",
			MaxStreamLen:  1000,
			TrimStrategy:  "MAXLEN",
			Retention:     10 * time.Minute,
		}
	})
	return testRedisConfig
}

func SetupRedis(t *testing.T) *redis.Client {
	testRedisConfig := TestRedisConfig()
	client := redis.NewClient(&redis.Options{
		Addr:     testRedisConfig.Addr,
		Password: testRedisConfig.Password,
		DB:       testRedisConfig.DB,
	})
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("client.Ping: %v", err)
	}
	return client
}

func CleanupRedis(t *testing.T, client *redis.Client) {
	err := client.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("client.FlushDB: %v", err)
	}
	client.Close()
}
