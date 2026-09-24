// redis.go adapts go-redis onto the cache's Store seam. The adapter carries
// no policy: timeouts, epoch rotation and fallback bounds live in Client.
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is the production Store over one go-redis client.
type RedisStore struct {
	client redis.UniversalClient
}

// NewRedisStore wraps a go-redis client (nil is refused).
func NewRedisStore(client redis.UniversalClient) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("cache: redis client is required")
	}
	return &RedisStore{client: client}, nil
}

// Get maps a Redis miss onto ErrNotFound; every other failure is returned
// untouched so the client can count it as a fallback.
func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	}
	return raw, nil
}

// Set writes with the given TTL (0 = no expiry; used only for the epoch
// sentinel, whose keys are epoch-scoped by construction).
func (s *RedisStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := s.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

// Del deletes the given keys.
func (s *RedisStore) Del(ctx context.Context, keys ...string) (int64, error) {
	n, err := s.client.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("cache del: %w", err)
	}
	return n, nil
}

// Scan walks all matching keys in bounded batches (SCAN + caller-side DEL).
func (s *RedisStore) Scan(ctx context.Context, match string, count int64, fn func(keys []string) error) error {
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, match, count).Result()
		if err != nil {
			return fmt.Errorf("cache scan: %w", err)
		}
		if len(keys) > 0 {
			if err := fn(keys); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// Exists reports whether key is present.
func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	n, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("cache exists: %w", err)
	}
	return n > 0, nil
}
