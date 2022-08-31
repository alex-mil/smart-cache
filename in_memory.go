package smartcahce

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
)

type memoryCacheItem struct {
	data      interface{}
	expiredAt time.Time
}

func (i memoryCacheItem) isExpired() bool {
	return i.expiredAt.Before(time.Now())
}

type InMemoryCache struct {
	decoratedCache iCache
	store          *ristretto.Cache
}

func NewInMemoryCache(cfg ristretto.Config, cache iCache) (*InMemoryCache, error) {
	c, err := ristretto.NewCache(&cfg)
	if err != nil {
		return nil, err
	}
	return &InMemoryCache{decoratedCache: cache, store: c}, nil
}

func NewDefaultInMemoryCache(cache iCache) (*InMemoryCache, error) {
	return NewInMemoryCache(ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	}, cache)
}

func (c InMemoryCache) Get(ctx context.Context, key string, deserializer Deserializer) (
	item interface{},
	found, isExpired bool,
	err error,
) {
	v, found := c.store.Get(key)
	if found {
		fromCache, ok := v.(memoryCacheItem)
		if !ok {
			return nil, false, false, fmt.Errorf("invalid in-memory cached item")
		}

		return fromCache.data, true, fromCache.isExpired(), nil
	}

	return c.decoratedCache.Get(ctx, key, deserializer)
}

func (c InMemoryCache) MGet(ctx context.Context, keys []string, deserializer Deserializer) (
	items map[string]interface{},
	expiredKeys []string,
	err error,
) {
	items = make(map[string]interface{}, len(keys))
	for _, key := range keys {
		fromCache, found, isExpired, e := c.Get(ctx, key, deserializer)
		if e != nil {
			return nil, nil, e
		}

		if found {
			items[key] = fromCache
		}

		if isExpired {
			expiredKeys = append(expiredKeys, key)
		}
	}

	if len(items) != len(keys) {
		keysToFetch := stringsPool.Get().([]string)
		defer func() {
			keysToFetch = keysToFetch[:0]
			stringsPool.Put(keysToFetch)
		}()

		for _, key := range keys {
			if _, ok := items[key]; !ok {
				keysToFetch = append(keysToFetch, key)
			}
		}

		fetchedItems, moreExpiredKeys, e := c.decoratedCache.MGet(ctx, keysToFetch, deserializer)
		if e != nil {
			return nil, nil, err
		}

		expiredKeys = append(expiredKeys, moreExpiredKeys...)
		for fetchedKey, fetchedItem := range fetchedItems {
			items[fetchedKey] = fetchedItem
		}
	}

	return items, expiredKeys, nil
}

func (c *InMemoryCache) Set(
	ctx context.Context,
	key string,
	item interface{},
	serializer Serializer,
	expiration, ttl time.Duration,
) error {
	newCachedItem := &memoryCacheItem{
		data:      item,
		expiredAt: time.Now().Add(expiration),
	}

	if !c.store.SetWithTTL(key, newCachedItem, 1, ttl) {
		return fmt.Errorf("failed to set in-memory cache key with TTL")
	}

	c.store.Wait()

	err := c.decoratedCache.Set(ctx, key, item, serializer, expiration, ttl)
	if err != nil {
		return fmt.Errorf("failed to set cache key with TTL")
	}

	return nil
}

func (c *InMemoryCache) Clear(ctx context.Context, key string) error {
	c.store.Del(key)
	return c.decoratedCache.Clear(ctx, key)
}
