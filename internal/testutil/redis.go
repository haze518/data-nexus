package testutil

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func SetupRedis(t testing.TB) *redis.Client {
	testRedisConfig := Config().RedisConfig
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

func CleanupRedis(t testing.TB, client *redis.Client) {
	err := client.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("client.FlushDB: %v", err)
	}
	client.Close()
}
