package smartcahce

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClusterLock struct {
	client *redis.ClusterClient
}

func NewRedisClusterLock(client *redis.ClusterClient) *RedisClusterLock {
	return &RedisClusterLock{client: client}
}

func (l *RedisClusterLock) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return l.client.SetNX(ctx, key, 1, ttl).Result()
}
