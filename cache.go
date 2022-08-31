package smartcahce

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	separator = ":"
)

var stringsPool = sync.Pool{
	New: func() interface{} { return make([]string, 0) },
}

type iLock interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

type iCache interface {
	Get(
		ctx context.Context,
		key string,
		deserializer Deserializer,
	) (item interface{}, found, isExpired bool, err error)
	MGet(
		ctx context.Context,
		keys []string,
		deserializer Deserializer,
	) (items map[string]interface{}, expiredKeys []string, err error)
	Set(
		ctx context.Context,
		key string,
		item interface{},
		serializer Serializer,
		expiration, ttl time.Duration,
	) error
	Clear(ctx context.Context, key string) error
}

type Serializer func(ctx context.Context, item interface{}) ([]byte, error)

type Deserializer func(ctx context.Context, data []byte) (interface{}, error)

type originalRetrieveFunc func(ctx context.Context) (interface{}, error)

type originalMultipleRetrieveFunc func(ctx context.Context, keys []string) (map[string]interface{}, error)

type Configuration struct {
	// Name is just a name indicator for metrics, logger. Usually it should be service name
	Name string
	// KeyPrefix is a prefix for all cache keys
	KeyPrefix string
	// Bypass disables cache
	Bypass bool
	// UseInMemoryCache enables or disables in-memory caching in addition to external caching
	UseInMemoryCache    bool
	InMemoryCacheClient *InMemoryCache
	// Lock prevents concurrent origin requests
	Lock iLock
}

type Cache struct {
	store             iCache
	lock              iLock
	cfg               Configuration
	singleFlightGroup singleflight.Group
}

func NewCache(c iCache, cfg Configuration) (*Cache, error) {
	var err error
	newCache := Cache{
		store:             c,
		lock:              cfg.Lock,
		cfg:               cfg,
		singleFlightGroup: singleflight.Group{},
	}

	if cfg.UseInMemoryCache {
		if cfg.InMemoryCacheClient == nil {
			cfg.InMemoryCacheClient, err = NewDefaultInMemoryCache(c)
			if err != nil {
				return nil, err
			}
		}
		newCache.store = cfg.InMemoryCacheClient
	}

	return &newCache, nil
}

func (c *Cache) LoadOrStore(
	ctx context.Context,
	key string,
	retrieveFn originalRetrieveFunc,
	serializer Serializer,
	deserializer Deserializer,
	opts CacheOperationOptions,
) (interface{}, error) {
	cacheKey := c.buildKey(key, opts.Version)
	ch := c.singleFlightGroup.DoChan(cacheKey, func() (interface{}, error) {
		if c.cfg.Bypass || opts.Bypass {
			return retrieveFn(ctx)
		}

		if opts.Override {
			return c.callRetrieveFnAndStoreInCache(ctx, cacheKey, retrieveFn, serializer, opts)
		}

		itemStoredInCache, found, isExpired, err := c.store.Get(
			ctx,
			cacheKey,
			decompress(deserializer, opts.Compression),
		)
		if err != nil {
			return c.callRetrieveFnAndStoreInCache(ctx, cacheKey, retrieveFn, serializer, opts)
		}

		if !found {
			return c.callRetrieveFnAndStoreInCache(ctx, cacheKey, retrieveFn, serializer, opts)
		}

		if isExpired {
			go func() {
				_, err, _ = c.singleFlightGroup.Do(cacheKey+"-async", func() (interface{}, error) {
					if opts.DataSourceLockTTL > 0 && c.lock != nil {
						ok := c.acquireLock(ctx, cacheKey, opts.DataSourceLockTTL)
						if !ok {
							// Someone has acquired the lock already. Stop execution.
							return nil, nil
						}
					}
					return c.callRetrieveFnAndStoreInCache(ctx, cacheKey, retrieveFn, serializer, opts)
				})
				if err != nil {
					log.Println(fmt.Errorf("failed to update cache item in async mode: %w", err))
				}
			}()
		}

		return itemStoredInCache, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		return result.Val, result.Err
	}
}

func (c *Cache) LoadOrStoreMultipleItems(
	ctx context.Context,
	keys []string,
	retrieveFn originalMultipleRetrieveFunc,
	serializer Serializer,
	deserializer Deserializer,
	opts CacheOperationOptions,
) ([]interface{}, error) {
	cacheKeys := c.buildKeys(keys, opts.Version)
	joinedCacheKeys := strings.Join(cacheKeys, "_")
	ch := c.singleFlightGroup.DoChan(joinedCacheKeys, func() (interface{}, error) {
		if c.cfg.Bypass || opts.Bypass {
			dataForCaching, err := retrieveFn(ctx, keys)
			if err != nil {
				return nil, err
			}

			return convertIntoSlice(dataForCaching), nil
		}

		if opts.Override {
			return c.callRetrieveFnAndMultiStoreInCache(ctx, cacheKeys, retrieveFn, serializer, opts, nil)
		}

		itemsStoredInCache, expiredKeys, err := c.store.MGet(ctx, cacheKeys, decompress(deserializer, opts.Compression))
		if err != nil {
			return c.callRetrieveFnAndMultiStoreInCache(ctx, cacheKeys, retrieveFn, serializer, opts, nil)
		}

		if len(expiredKeys) > 0 {
			go func() {
				_, err, _ = c.singleFlightGroup.Do(joinedCacheKeys+"-async", func() (interface{}, error) {
					return c.callRetrieveFnAndMultiStoreInCache(
						context.Background(),
						expiredKeys,
						retrieveFn,
						serializer,
						opts,
						nil,
					)
				})
				if err != nil {
					log.Println(fmt.Errorf("failed to update cache item in async mode: %w", err))
				}
			}()
		}

		if len(itemsStoredInCache) != len(cacheKeys) {
			missingCacheKeys := stringsPool.Get().([]string)
			defer func() {
				missingCacheKeys = missingCacheKeys[:0]
				stringsPool.Put(missingCacheKeys)
			}()

			for _, cacheKey := range cacheKeys {
				if _, ok := itemsStoredInCache[cacheKey]; !ok {
					missingCacheKeys = append(missingCacheKeys, cacheKey)
				}
			}

			return c.callRetrieveFnAndMultiStoreInCache(
				ctx,
				missingCacheKeys,
				retrieveFn,
				serializer,
				opts,
				convertIntoSlice(itemsStoredInCache),
			)
		}

		return convertIntoSlice(itemsStoredInCache), nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		return result.Val.([]interface{}), result.Err
	}
}

func (c *Cache) Clear(ctx context.Context, key, version string) error {
	cacheKey := c.buildKey(key, version)
	if err := c.store.Clear(ctx, cacheKey); err != nil {
		return fmt.Errorf("failed to clear item in cache: %w", err)
	}
	return nil
}

func (c *Cache) callRetrieveFnAndStoreInCache(
	ctx context.Context,
	cacheKey string,
	retrieveFn originalRetrieveFunc,
	serializer Serializer,
	opts CacheOperationOptions,
) (interface{}, error) {
	return func(
		ctx context.Context,
		cacheKey string,
		fn func(ctx context.Context) (interface{}, error),
		serializationFn Serializer,
		opts CacheOperationOptions,
	) (interface{}, error) {
		dataForCaching, err := fn(ctx)
		if err != nil {
			return nil, err
		}

		refreshAfter, keyTTL := opts.RefreshAfter, opts.KeyTTL
		if opts.ExpirationFn != nil {
			refreshAfter, keyTTL = opts.ExpirationFn(dataForCaching)
		}

		if err = c.store.Set(
			ctx,
			cacheKey,
			dataForCaching,
			compress(serializationFn, opts.Compression),
			refreshAfter,
			keyTTL,
		); err != nil {
			return nil, fmt.Errorf("failed to store in the cache: %w", err)
		}
		return dataForCaching, nil
	}(ctx, cacheKey, retrieveFn, serializer, opts)
}

func (c *Cache) callRetrieveFnAndMultiStoreInCache(
	ctx context.Context,
	cacheKeys []string,
	retrieveFn originalMultipleRetrieveFunc,
	serializer Serializer,
	opts CacheOperationOptions,
	items []interface{},
) ([]interface{}, error) {
	dataForCaching, err := retrieveFn(ctx, c.removeKeysPrefix(cacheKeys, opts.Version))
	if err != nil {
		return nil, err
	}

	if items == nil {
		items = make([]interface{}, 0, len(dataForCaching))
	}

	for key, item := range dataForCaching {
		if err = c.store.Set(
			ctx,
			c.buildKey(key, opts.Version),
			item,
			compress(serializer, opts.Compression),
			opts.RefreshAfter,
			opts.KeyTTL,
		); err != nil {
			return nil, fmt.Errorf("failed to store item to driver: %w", err)
		}

		items = append(items, item)
	}
	return items, nil
}

func (c *Cache) acquireLock(ctx context.Context, cacheKey string, ttl time.Duration) (isAcquired bool) {
	lockKey := fmt.Sprintf("%s%s%s", cacheKey, separator, "lock")

	isAcquired, err := c.lock.Lock(ctx, lockKey, ttl)
	if err != nil {
		// Something went wrong with locking functionality.
		// Continue execution to keep the main flow working.
		log.Println(fmt.Errorf("failed to take a lock: %w", err))
	}
	return isAcquired
}

func (c *Cache) buildKey(key, version string) string {
	return fmt.Sprintf("%s%s%s%s%s", c.cfg.KeyPrefix, separator, version, separator, key)
}

func (c *Cache) buildKeys(keys []string, version string) []string {
	results := make([]string, len(keys))
	for i, key := range keys {
		results[i] = c.buildKey(key, version)
	}
	return results
}

func (c *Cache) removeKeysPrefix(keys []string, version string) []string {
	results := make([]string, len(keys))
	for i, key := range keys {
		results[i] = strings.TrimPrefix(key, c.cfg.KeyPrefix+separator+version+separator)
	}

	return results
}

func compress(serializationFn Serializer, compressionType CompressionType) Serializer {
	return func(ctx context.Context, item interface{}) ([]byte, error) {
		b, err := serializationFn(ctx, item)
		if err != nil {
			return nil, err
		}

		if compressionType != GzipCompression {
			return b, err
		}

		buffer := bytes.NewBuffer(nil)
		writerLevel, err := gzip.NewWriterLevel(buffer, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}

		if _, err = writerLevel.Write(b); err != nil {
			return nil, err
		}

		if err = writerLevel.Close(); err != nil {
			return nil, err
		}

		return buffer.Bytes(), nil
	}
}

func decompress(deserializationFn Deserializer, compressionType CompressionType) Deserializer {
	return func(ctx context.Context, data []byte) (interface{}, error) {
		if compressionType == GzipCompression {
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}

			b, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}

			data = b
		}
		return deserializationFn(ctx, data)
	}
}

func convertIntoSlice(m map[string]interface{}) []interface{} {
	results := make([]interface{}, 0, len(m))
	for _, value := range m {
		results = append(results, value)
	}
	return results
}
