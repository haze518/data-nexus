package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Addr          string
	Password      string
	DB            int
	PoolSize      int
	StreamName    string
	ConsumerGroup string
	ConsumerID    string
	MaxStreamLen  int64
	TrimStrategy  string
	Retention     time.Duration
}

type RedisBroker struct {
	client               *redis.Client
	config               RedisConfig
	log                  *logging.Logger
	consumerGroupCreated bool
	serverName string
}

func NewRedisBroker(ctx context.Context, config RedisConfig, logger *logging.Logger, serverName string) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
		PoolSize: config.PoolSize,
	})
	return &RedisBroker{client, config, logger, false, serverName}, nil
}

func (b *RedisBroker) Publish(ctx context.Context, val *types.Metric) error {
	pbval, err := types.Marshal(val)
	if err != nil {
		return fmt.Errorf("toProto: %w", err)
	}
	_, err = b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.config.StreamName,
		ID:     "*",
		Values: map[string]interface{}{
			"data": pbval,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("client.XAdd: %w", err)
	}
	return nil
}

func (b *RedisBroker) Consume(ctx context.Context, n int64) ([]*types.Metric, error) {
	err := b.ensureGroupExists(ctx)
	if err != nil {
		return nil, fmt.Errorf("s.ensureGroupExists: %w", err)
	}
	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{b.config.StreamName, ">"},
		Group:    b.config.ConsumerGroup,
		Consumer: b.config.ConsumerID,
		Count:    n,
		Block:    5000 * time.Millisecond,
		NoAck:    false,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("client.XReadGroup: %w", err)
	}

	result := make([]*types.Metric, 0, n)
	for _, stream := range streams {
		for _, data := range stream.Messages {
			rawData, ok := data.Values["data"].(string)
			if !ok {
				b.log.Error("skip value due to incorrect format")
				continue
			}
			metric, err := types.Unmarshal([]byte(rawData))
			if err != nil {
				return nil, fmt.Errorf("unmarshal: %w", err)
			}
			result = append(result, metric)
		}
	}
	return result, nil
}

func (b *RedisBroker) SetServerState(ctx context.Context, state types.ServerState, ttl time.Duration) error {
	// TODO: Consider the necessity of TTL
	return b.client.Set(ctx, b.serverName, state, ttl).Err()
}

func (b *RedisBroker) ensureGroupExists(ctx context.Context) error {
	if b.consumerGroupCreated {
		return nil
	}
	groups, err := b.client.XInfoGroups(ctx, b.config.StreamName).Result()
	if err != nil {
		return fmt.Errorf("client.XInfoGroups: %w", err)
	}
	var groupExists bool
	for _, g := range groups {
		if g.Name == b.config.ConsumerGroup {
			groupExists = true
			break
		}
	}

	if !groupExists {
		b.log.Info(fmt.Sprintf("Consumer group %v does not exist, creating...", b.config.ConsumerGroup))
		err = b.client.XGroupCreate(ctx, b.config.StreamName, b.config.ConsumerGroup, "0").Err()
		if err != nil {
			return fmt.Errorf("client.XGroupCreate: %w", err)
		}
		b.consumerGroupCreated = true
	}
	return nil
}
