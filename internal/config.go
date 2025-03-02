package internal

import "time"

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
