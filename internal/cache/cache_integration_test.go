//go:build integration_redis

// cache_integration_test.go is the T065 acceptance (Integration-Redis;
// V-CACHE): cache-aside behavior against a real Redis, event-driven
// invalidation leaving 0 stale financial authority, bounded PostgreSQL
// fallback while Redis is unavailable, epoch rotation making pre-rotation
// values unreachable, and the freshness annotation.
package cache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// recordingObserver records cache metric calls.
type recordingObserver struct {
	mu         sync.Mutex
	hits       int
	misses     int
	fallbacks  int
	rotations  int
	lastFamily string
}

func (o *recordingObserver) ObserveCacheHit(family string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hits++
	o.lastFamily = family
}

func (o *recordingObserver) ObserveCacheMiss(family string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.misses++
	o.lastFamily = family
}

func (o *recordingObserver) ObserveCacheFallback(family string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fallbacks++
	o.lastFamily = family
}

func (o *recordingObserver) ObserveCacheEpochRotation() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rotations++
}

func (o *recordingObserver) snapshot() (hits, misses, fallbacks, rotations int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hits, o.misses, o.fallbacks, o.rotations
}

// t065Client builds a real-Redis cache client plus its raw go-redis handle.
func t065Client(t *testing.T, addr string, observer Observer) (*Client, *redis.Client) {
	t.Helper()
	opt, err := redis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis addr %q: %v", addr, err)
	}
	raw := redis.NewClient(opt)
	t.Cleanup(func() { _ = raw.Close() })
	store, err := NewRedisStore(raw)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	client, err := NewClient(store, Config{
		Epoch:                  "1",
		TTL:                    30 * time.Second,
		Timeout:                2 * time.Second,
		MaxFallbackConcurrency: 2,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.SetObserver(observer)
	return client, raw
}

func TestCacheVCacheAgainstRealRedis(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })

	observer := &recordingObserver{}
	client, raw := t065Client(t, redisCtr.Addr(), observer)
	inv, err := NewInvalidator(client)
	if err != nil {
		t.Fatalf("NewInvalidator: %v", err)
	}

	// 1. Cache-aside: a miss reads the authority once and caches it; the next
	// read is a hit.
	authoritativeVersion := int64(1)
	authoritativeValue := []byte("v1")
	loads := 0
	load := func(context.Context) ([]byte, int64, error) {
		loads++
		return append([]byte(nil), authoritativeValue...), authoritativeVersion, nil
	}
	first, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", authoritativeVersion, load)
	if err != nil || loads != 1 || string(first.Value) != "v1" {
		t.Fatalf("first GetOrLoad = (%+v, %v) loads=%d, want the authoritative v1", first, err, loads)
	}
	second, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", authoritativeVersion, load)
	if err != nil || loads != 1 || second.Freshness != FreshnessFresh {
		t.Fatalf("second GetOrLoad = (%+v, %v) loads=%d, want a fresh hit", second, err, loads)
	}

	// 2. Authority changes and the event-driven invalidator deletes the
	// affected range: the next read reloads the new authority (0 stale).
	authoritativeVersion = 2
	authoritativeValue = []byte("v2")
	inv.Invalidate(ctx, []Ref{{Family: FamilyDeposit, ID: "obs-1"}})
	third, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", authoritativeVersion, load)
	if err != nil {
		t.Fatalf("third GetOrLoad: %v", err)
	}
	if string(third.Value) != "v2" || loads != 2 {
		t.Fatalf("after invalidation = %q loads=%d, want v2 reloaded (no stale authority)", third.Value, loads)
	}
	// A known watermark ahead of the cached version is labeled possibly_stale.
	stale, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 3, load)
	if err != nil || stale.Freshness != FreshnessPossiblyStale {
		t.Fatalf("watermark-ahead read = (%+v, %v), want possibly_stale", stale, err)
	}

	// 3. Redis outage: reads fall back to the authority behind the bounded
	// guard; the fallback is counted and the guard ceiling holds.
	if err := redisCtr.Stop(ctx); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	var inFlight, maxInFlight atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := client.GetOrLoad(ctx, FamilyWithdrawal, "w-"+string(rune('a'+i)), 1, func(context.Context) ([]byte, int64, error) {
				current := inFlight.Add(1)
				for {
					max := maxInFlight.Load()
					if current <= max || maxInFlight.CompareAndSwap(max, current) {
						break
					}
				}
				time.Sleep(30 * time.Millisecond)
				inFlight.Add(-1)
				return []byte("authority"), 1, nil
			})
			if err != nil {
				t.Errorf("fallback GetOrLoad: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("max concurrent fallback loads = %d, want <= 2 (semaphore)", got)
	}
	if _, _, fallbacks, _ := observer.snapshot(); fallbacks == 0 {
		t.Fatal("Redis outage produced no fallback observations")
	}
	// The authority stays reachable with a fresh answer while Redis is down.
	fallback, err := client.GetOrLoad(ctx, FamilyWithdrawal, "w-fallback", 1, load)
	if err != nil || string(fallback.Value) != "v2" {
		t.Fatalf("fallback read = (%+v, %v), want the authoritative value", fallback, err)
	}

	// 4. Recovery with a cleared namespace: the sentinel is gone, so the
	// epoch rotates and pre-rotation values become unreachable (0 stale).
	if err := redisCtr.Start(ctx); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := raw.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("redis never recovered")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Prime a value under the current epoch, then simulate the clear by
	// removing only the epoch sentinel (the primed key physically remains).
	if _, err := client.GetOrLoad(ctx, FamilyExecution, "intent-1", 1, func(context.Context) ([]byte, int64, error) {
		return []byte("prime"), 1, nil
	}); err != nil {
		t.Fatalf("prime before rotation: %v", err)
	}
	oldKey := client.Key(FamilyExecution, "intent-1")
	oldEpoch := client.CurrentEpoch()
	if err := raw.Del(ctx, "txharbor:"+oldEpoch+":epoch").Err(); err != nil {
		t.Fatalf("delete sentinel: %v", err)
	}
	if !client.EnsureEpoch(ctx) {
		t.Fatal("EnsureEpoch = false after the sentinel disappeared")
	}
	if client.CurrentEpoch() == oldEpoch {
		t.Fatal("EnsureEpoch did not rotate after the namespace was cleared")
	}
	if n, err := raw.Exists(ctx, oldKey).Result(); err != nil || n != 1 {
		t.Fatalf("old key present=%d err=%v, want the physical key to remain (unreachable, not deleted)", n, err)
	}
	if _, ok := client.Get(ctx, FamilyExecution, "intent-1"); ok {
		t.Fatal("pre-rotation value still reachable after the epoch rotation")
	}
	if _, _, _, rotations := observer.snapshot(); rotations == 0 {
		t.Fatal("epoch rotation was never observed")
	}

	// 5. A post-rotation read is a clean miss and reloads the authority.
	post, err := client.GetOrLoad(ctx, FamilyExecution, "intent-1", 1, func(context.Context) ([]byte, int64, error) {
		return []byte("post-rotation"), 1, nil
	})
	if err != nil || string(post.Value) != "post-rotation" {
		t.Fatalf("post-rotation read = (%+v, %v), want the fresh authority", post, err)
	}
	if !strings.Contains(client.Key(FamilyExecution, "intent-1"), client.CurrentEpoch()) {
		t.Fatal("key does not carry the rotated epoch")
	}
}

// TestCacheStoreAdapterMapsMissAndTTL pins the Redis adapter contract: a miss
// is ErrNotFound (never a fallback) and cached values carry the TTL bound.
func TestCacheStoreAdapterMapsMissAndTTL(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })

	client, raw := t065Client(t, redisCtr.Addr(), nil)
	if _, err := client.GetOrLoad(ctx, FamilyDeposit, "ttl-1", 1, func(context.Context) ([]byte, int64, error) {
		return []byte("v"), 1, nil
	}); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	pttl, err := raw.PTTL(ctx, client.Key(FamilyDeposit, "ttl-1")).Result()
	if err != nil || pttl <= 0 || pttl > 30*time.Second {
		t.Fatalf("PTTL = %v err=%v, want a positive TTL bound <= 30s", pttl, err)
	}
	if _, err := client.store.Get(ctx, client.Key(FamilyDeposit, "missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("adapter miss = %v, want ErrNotFound", err)
	}
}
