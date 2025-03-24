package broker

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/haze518/data-nexus/internal/logging"
	"github.com/haze518/data-nexus/internal/types"
	"github.com/haze518/data-nexus/pkg/config"
	"github.com/redis/go-redis/v9"
)

// RedisBroker is an implementation of the Broker interface using Redis Streams.
type RedisBroker struct {
	client               *redis.Client      // Redis client instance
	config               config.RedisConfig // Configuration for Redis connection and stream setup
	log                  *logging.Logger    // Logger for internal logging
	consumerGroupCreated atomic.Bool        // Ensures consumer group is only created once
	streamCreated        atomic.Bool        // Ensures stream is created before operations
}

// NewRedisBroker initializes a new RedisBroker with the given config and logger.
// It returns an error if Redis client creation fails.
func NewRedisBroker(config config.RedisConfig, logger *logging.Logger) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
		PoolSize: config.PoolSize,
	})
	return &RedisBroker{client, config, logger, atomic.Bool{}, atomic.Bool{}}, nil
}

// Close gracefully closes the Redis client connection.
func (b *RedisBroker) Close() error {
	return b.client.Close()
}

// Publish adds a single message to the Redis stream.
func (b *RedisBroker) Publish(ctx context.Context, val []byte) (string, error) {
	id, err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.config.StreamName,
		ID:     "*",
		Values: map[string]interface{}{
			"data": val,
		},
	}).Result()
	if !b.streamCreated.Load() {
		b.streamCreated.Store(true)
	}
	if err != nil {
		return "", fmt.Errorf("client.XAdd: %w", err)
	}
	return id, nil
}

// PublishBatch publishes multiple messages to the Redis stream in a pipeline.
// Returns the IDs of published messages.
func (b *RedisBroker) PublishBatch(ctx context.Context, vals [][]byte) ([]string, error) {
	tx := b.client.TxPipeline()
	ids := make([]string, 0, len(vals))

	for _, val := range vals {
		cmd := tx.XAdd(ctx, &redis.XAddArgs{
			Stream: b.config.StreamName,
			ID:     "*",
			Values: map[string]interface{}{"data": val},
		})
		ids = append(ids, cmd.Val())
	}
	if !b.streamCreated.Load() {
		b.streamCreated.Store(true)
	}

	_, err := tx.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("tx.Exec: %w", err)
	}
	return ids, nil
}

// Consume reads up to 'n' messages from the Redis stream using consumer groups.
// It blocks for up to 5 seconds if no messages are available.
func (b *RedisBroker) Consume(n int64) ([]*types.Metric, error) {
	err := b.ensureGroupExists()
	if err != nil {
		return nil, fmt.Errorf("s.ensureGroupExists: %w", err)
	}
	streams, err := b.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Streams:  []string{b.config.StreamName, ">"},
		Group:    b.config.ConsumerGroup,
		Consumer: b.config.ConsumerID,
		Count:    n,
		Block:    5 * time.Second,
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
			metric, err := types.Unmarshal(rawData)
			if err != nil {
				return nil, fmt.Errorf("unmarshal: %w", err)
			}
			metric.ID = &data.ID
			result = append(result, metric)
		}
	}
	return result, nil
}

// Ack acknowledges the successful processing of messages by ID.
func (b *RedisBroker) Ack(ids ...string) error {
	return b.client.XAck(context.Background(), b.config.StreamName, b.config.ConsumerGroup, ids...).Err()
}

// SetServerState sets the current server's state (active/inactive) with a TTL.
func (b *RedisBroker) SetServerState(state types.ServerState, ttl time.Duration) error {
	msg := fmt.Sprintf("state:%s", b.config.ConsumerID)
	return b.client.Set(context.Background(), msg, state.String(), ttl).Err()
}

// ListServers returns the states of all known servers in the system,
// based on Redis key pattern `state:*`.
func (b *RedisBroker) ListServers() (map[string]types.ServerState, error) {
	var cursor uint64
	serverStates := make(map[string]types.ServerState)

	for {
		keys, newCursor, err := b.client.Scan(context.Background(), cursor, "state:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("client.Scan: %w", err)
		}

		if len(keys) > 0 {
			values, err := b.client.MGet(context.Background(), keys...).Result()
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

// MoveInactiveServerMsgs reclaims unacknowledged messages from an inactive server
// using Lua scripting for atomic execution. Only one Redis client can claim
// messages from a given inactive server at a time.
func (b *RedisBroker) MoveInactiveServerMsgs(inactiveSrv string, batchSize int) ([]*types.Metric, error) {
	if inactiveSrv == b.config.ConsumerID {
		return nil, nil
	}
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

	result, err := script.Run(context.Background(), b.client, keys, args...).Result()
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

		metric, err := types.Unmarshal(rawData)
		if err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		metric.ID = &messageID

		metrics = append(metrics, metric)
	}
	return metrics, nil
}

// ensureGroupExists ensures that the Redis consumer group exists.
// If it doesn't, the group is created. This method is idempotent.
func (b *RedisBroker) ensureGroupExists() error {
	if !b.streamCreated.Load() {
		return fmt.Errorf("stream is not created yet, need init publish")
	}
	if b.consumerGroupCreated.Load() {
		return nil
	}
	groups, err := b.client.XInfoGroups(context.Background(), b.config.StreamName).Result()
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
		err = b.client.XGroupCreate(context.Background(), b.config.StreamName, b.config.ConsumerGroup, "0").Err()
		if err != nil {
			return fmt.Errorf("client.XGroupCreate: %w", err)
		}
		b.consumerGroupCreated.Store(true)
	}
	return nil
}
