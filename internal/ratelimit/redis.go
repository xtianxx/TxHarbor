// redis.go adapts go-redis onto the limiter's ScriptStore seam. The adapter
// carries no policy: unavailability classification lives in Limiter.
package ratelimit

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisScriptStore is the production ScriptStore over one go-redis client.
type RedisScriptStore struct {
	client redis.UniversalClient
}

// NewRedisScriptStore wraps a go-redis client (nil is refused).
func NewRedisScriptStore(client redis.UniversalClient) (*RedisScriptStore, error) {
	if client == nil {
		return nil, errors.New("ratelimit: redis client is required")
	}
	return &RedisScriptStore{client: client}, nil
}

// Eval runs one atomic script evaluation and returns the raw Redis reply; the
// limiter validates its shape before trusting it.
func (s *RedisScriptStore) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	result, err := s.client.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("ratelimit eval: %w", err)
	}
	return result, nil
}
