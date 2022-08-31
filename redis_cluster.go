package smartcahce

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// SameSlotMode means that all keys of this client will have the same slot and will be located on the same node.
	// Please use this strategy if you don't have huge amount of data and can't overload a specific node.
	SameSlotMode = "THE_SAME_SLOT"

	// MultipleGetMode means that we will use multi get operations to get all records.
	// It less performant but all keys will be split between nodes.
	MultipleGetMode = "MULTIPLE_GET"

	// MultipleParallelGetMode means that we will use multi get operations to get all records in parallel.
	MultipleParallelGetMode = "MULTIPLE_PARALLEL_GET"

	DefaultMode     = MultipleParallelGetMode
	DefaultTimeout  = time.Second * 2
	DefaultPoolSize = 100
)

// MGetMode controls how we achieve multi GET in Redis Cluster.
// All keys in Redis Cluster are split between nodes and which node use will be decided by key slot.
// MGet will work only when all keys passed will have the same slot. This is how Redis works.
type MGetMode string

type RedisCfg struct {
	// MultiGetMode is as strategy we want to use for multi get operation.
	MultiGetMode MGetMode

	// Used and required if multi get mode equals SameSlotMode.
	Slot string

	// PoolSize is size of pool for redis connections.
	PoolSize uint

	// Timeout will be used as read/write timeout for opened redis connection.
	Timeout time.Duration
}

type RedisClusterCache struct {
	store        *redis.ClusterClient
	multiGetMode MGetMode
	slot         string
	rateLimiter  chan struct{}
}

func NewRedisClusterCache(addr string, cfg RedisCfg) (*RedisClusterCache, error) {
	if cfg.MultiGetMode == "" {
		cfg.MultiGetMode = DefaultMode
	}

	if cfg.PoolSize == 0 {
		cfg.PoolSize = DefaultPoolSize
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}

	if cfg.MultiGetMode == SameSlotMode && cfg.Slot == "" {
		return nil, errors.New("please specify slot for keys")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:         strings.Split(addr, ","),
		PoolSize:      int(cfg.PoolSize),
		WriteTimeout:  cfg.Timeout,
		ReadTimeout:   cfg.Timeout,
		RouteRandomly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	rateLimiter := make(chan struct{}, cfg.PoolSize)
	for i := uint(0); i < cfg.PoolSize; i++ {
		rateLimiter <- struct{}{}
	}

	return &RedisClusterCache{
		store:        client,
		multiGetMode: cfg.MultiGetMode,
		slot:         cfg.Slot,
		rateLimiter:  rateLimiter,
	}, nil
}

func (c RedisClusterCache) Get(ctx context.Context, key string, deserializer Deserializer) (
	item interface{},
	found, isExpired bool,
	err error,
) {
	v, err := c.store.Get(ctx, c.buildKey(key)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}

	cacheItem := &redisCacheItem{}
	if err = cacheItem.deserialize([]byte(v)); err != nil {
		return nil, false, false, err
	}

	itemStoredInCache, err := deserializer(ctx, cacheItem.data)
	if err != nil {
		return nil, false, false, err
	}

	return itemStoredInCache, true, cacheItem.isExpired(), nil
}

func (c RedisClusterCache) MGet(ctx context.Context, keys []string, deserializer Deserializer) (
	items map[string]interface{},
	expiredKeys []string,
	err error,
) {
	mGetResults, err := c.mGet(ctx, keys)
	if err != nil {
		return nil, nil, err
	}

	cacheItem := &redisCacheItem{}
	itemsStoredInCache := make(map[string]interface{}, len(keys))
	for _, mGetResult := range mGetResults {
		rawBytes, isBytes := mGetResult.([]byte)
		if !isBytes {
			rawString, isString := mGetResult.(string)
			if !isString {
				continue
			}

			rawBytes = []byte(rawString)
		}

		if err = cacheItem.deserialize(rawBytes); err != nil {
			return nil, nil, err
		}

		itemStoredInCache, deserializeErr := deserializer(ctx, cacheItem.data)
		if deserializeErr != nil {
			return nil, nil, deserializeErr
		}

		if cacheItem.isExpired() {
			expiredKeys = append(expiredKeys, cacheItem.key)
		}

		itemsStoredInCache[cacheItem.key] = itemStoredInCache
	}
	return itemsStoredInCache, expiredKeys, nil
}

func (c *RedisClusterCache) Set(
	ctx context.Context,
	key string,
	item interface{},
	serializer Serializer,
	expiration, ttl time.Duration,
) error {
	rawBytes, err := serializer(ctx, item)
	if err != nil {
		return err
	}

	cacheItem := &redisCacheItem{
		key:       key,
		data:      rawBytes,
		expiredAt: time.Now().Add(expiration),
	}
	serializedCacheItem := cacheItem.serialize()

	return c.store.Set(ctx, c.buildKey(key), serializedCacheItem, ttl).Err()
}

func (c *RedisClusterCache) Clear(ctx context.Context, key string) error {
	return c.store.Del(ctx, c.buildKey(key)).Err()
}

func (c RedisClusterCache) mGet(ctx context.Context, keys []string) ([]interface{}, error) {
	switch c.multiGetMode {
	case MultipleGetMode:
		results := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			getResult, err := c.store.Get(ctx, c.buildKey(key)).Result()
			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("failed to get key %q: %w", key, err)
			}
			results = append(results, getResult)
		}
		return results, nil
	case SameSlotMode:
		return c.store.MGet(ctx, c.buildKeys(keys)...).Result()
	case MultipleParallelGetMode:
		redisChan := make(chan *redis.StringCmd)
		doneChan := make(chan struct{})
		defer close(redisChan)
		defer close(doneChan)

		for _, key := range keys {
			<-c.rateLimiter
			go func(key string) {
				defer func() {
					c.rateLimiter <- struct{}{}
				}()

				select {
				case <-doneChan:
				case redisChan <- c.store.Get(ctx, c.buildKey(key)):
				}
			}(key)
		}

		results := make([]interface{}, 0, len(keys))
		for i := 0; i < len(keys); i++ {
			redisResult := <-redisChan
			if errors.Is(redisResult.Err(), redis.Nil) {
				continue
			}
			if redisResult.Err() != nil {
				close(doneChan) // stop to accept other values
				return nil, fmt.Errorf("failed to get item: %w", redisResult.Err())
			}
			results = append(results, redisResult.Val())
		}
		return results, nil
	default:
		return nil, fmt.Errorf("multi get mode %q not supported", c.multiGetMode)
	}
}

func (c RedisClusterCache) buildKey(key string) string {
	if c.multiGetMode == SameSlotMode {
		return fmt.Sprintf("{%s}%s", c.slot, key)
	}
	return key
}

func (c RedisClusterCache) buildKeys(keys []string) []string {
	results := make([]string, 0, len(keys))
	for _, key := range keys {
		results = append(results, c.buildKey(key))
	}
	return results
}

type redisCacheItem struct {
	key       string
	data      []byte
	expiredAt time.Time
}

func (i redisCacheItem) isExpired() bool {
	return i.expiredAt.Before(time.Now())
}

func (i redisCacheItem) serialize() []byte {
	buffer := make([]byte, 10, 10+len(i.key)+len(i.data))
	binary.LittleEndian.PutUint64(buffer[0:8], uint64(i.expiredAt.Unix()))
	binary.LittleEndian.PutUint16(buffer[8:10], uint16(len(i.key)))
	buffer = append(buffer, []byte(i.key)...)
	buffer = append(buffer, i.data...)

	return buffer
}

func (i *redisCacheItem) deserialize(bytes []byte) error {
	if len(bytes) < 10 {
		return fmt.Errorf("invalid commressed cache item value")
	}

	i.expiredAt = time.Unix(int64(binary.LittleEndian.Uint64(bytes[:8])), 0)
	keyLen := binary.LittleEndian.Uint16(bytes[8:10])
	i.key = string(bytes[10 : 10+keyLen])
	i.data = bytes[10+keyLen:]

	return nil
}
