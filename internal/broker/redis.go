package broker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/haze518/data-nexus/internal"
	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client               *redis.Client
	config               internal.RedisConfig
	log                  *logging.Logger
	consumerGroupCreated bool
}

func NewRedisBroker(ctx context.Context, config internal.RedisConfig, logger *logging.Logger) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
		PoolSize: config.PoolSize,
	})
	return &RedisBroker{client, config, logger, false}, nil
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
	msg := fmt.Sprintf("state:%s", b.config.ConsumerID)
	return b.client.Set(ctx, msg, state.String(), ttl).Err()
}

func (b *RedisBroker) ListServers(ctx context.Context) (map[string]types.ServerState, error) {
	var cursor uint64
	serverStates := make(map[string]types.ServerState)

	for {
		keys, newCursor, err := b.client.Scan(ctx, cursor, "state:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("client.Scan: %w", err)
		}

		if len(keys) > 0 {
			values, err := b.client.MGet(ctx, keys...).Result()
			if err != nil {
				return nil, fmt.Errorf("client.MGet: %w", err)
			}

			for i, key := range keys {
				state, ok := values[i].(string)
				if !ok {
					continue
				}
				serverName := strings.TrimPrefix(key, "state:")
				serverStates[serverName] = types.ServerState(types.ParseServerState(state))
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	return serverStates, nil
}

func (b *RedisBroker) MoveInactiveServerMsgs(ctx context.Context, inactiveSrv string, batchSize int) ([]*types.Metric, error) {
	script := redis.NewScript(`
		local stateKey = KEYS[1]
		local lockKey = KEYS[2]
		local streamName = KEYS[3]

		local consumerGroup = ARGV[1]
		local newConsumer = ARGV[2]
		local maxBatchSize = tonumber(ARGV[3])
		local now = tonumber(ARGV[4])
		local deadConsumer = ARGV[5]

		local state = redis.call("GET", stateKey)
		if state ~= "inactive" then
			return {}
		end

		if redis.call("SETNX", lockKey, "locked") == 0 then
			return {}
		end
		redis.call("EXPIRE", lockKey, 10)

		local pendingMessages = redis.call("XPENDING", streamName, consumerGroup, 0, "+", maxBatchSize, deadConsumer)
		if #pendingMessages == 0 then
			redis.call("DEL", lockKey)
			redis.call("DEL", stateKey)
			return {}
		end

		local messageIDs = {}
		for _, msg in ipairs(pendingMessages) do
			table.insert(messageIDs, msg[1])
		end

		local claimedMessages = redis.call("XCLAIM", streamName, consumerGroup, newConsumer, 0, unpack(messageIDs))
		if #claimedMessages == 0 then
			redis.call("DEL", lockKey)
			return {}
		end

		redis.call("DEL", lockKey)
		return claimedMessages
	`)

	keys := []string{
		fmt.Sprintf("state:%s", inactiveSrv),
		fmt.Sprintf("lock:%s", inactiveSrv),
		b.config.StreamName,
	}

	args := []interface{}{b.config.ConsumerGroup, b.config.ConsumerID, batchSize, time.Now().Unix(), inactiveSrv}

	result, err := script.Run(ctx, b.client, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("script.Run: %w", err)
	}

	claimedMessages, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}

	metrics := make([]*types.Metric, 0, len(claimedMessages))
	for _, msg := range claimedMessages {
		msgSlice, ok := msg.([]interface{})
		if !ok || len(msgSlice) < 2 {
			continue
		}

		messageID, ok := msgSlice[0].(string)
		if !ok {
			continue
		}
		b.log.Debug("MoveInactiveServerMsgs got message with id", messageID)

		fieldData, ok := msgSlice[1].([]interface{})
		if !ok {
			continue
		}

		rawData, ok := fieldData[1].(string)
		if !ok {
			continue
		}

		metric, err := types.Unmarshal([]byte(rawData))
		if err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}

		metrics = append(metrics, metric)
	}
	return metrics, nil
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
