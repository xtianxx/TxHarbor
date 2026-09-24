// Package cache implements the 013 non-authoritative read-model cache
// (contracts/redis.md §2; FR-17/FR-23; research R10). It is cache-aside only:
//
//   - keys are namespaced `txharbor:<epoch>:<family>:<id>`; a Redis
//     clear/rebuild rotates the epoch so pre-rotation values become
//     unreachable;
//   - values carry `(value, source_version, cached_at)` and every read is
//     annotated `fresh` or `possibly_stale` — the cache never pretends to be
//     authoritative;
//   - invalidation is driven by authority-transition events (invalidator.go;
//     no dual write) with TTL as the fallback bound;
//   - Redis unavailable/timeout/untrusted falls back to a direct PostgreSQL
//     read behind a bounded guard (singleflight + concurrency semaphore +
//     timeout): an outage can never stampede the authority;
//   - funding decisions never read this package (contracts/redis.md §2.1).
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Family is a closed non-authoritative read-model family. Families are
// low-cardinality display/aggregate groupings; financial authority never
// resolves through them.
type Family string

const (
	// FamilyDeposit is the deposit observation/confirmation read model.
	FamilyDeposit Family = "deposit"
	// FamilyWithdrawal is the withdrawal request read model.
	FamilyWithdrawal Family = "withdrawal"
	// FamilyExecution is the withdrawal execution read model.
	FamilyExecution Family = "execution"
)

// Valid reports whether f is a declared family.
func (f Family) Valid() bool {
	switch f {
	case FamilyDeposit, FamilyWithdrawal, FamilyExecution:
		return true
	}
	return false
}

// Freshness is the response annotation of one cache read (contracts/redis.md
// §2.3). A value whose source version cannot be confirmed against the known
// authoritative watermark is `possibly_stale` and MUST be labeled as such.
type Freshness string

const (
	// FreshnessFresh: the cached source version is at or beyond the known
	// authoritative watermark.
	FreshnessFresh Freshness = "fresh"
	// FreshnessPossiblyStale: the value may lag authority (unknown watermark,
	// older source version, or a Redis that could not be consulted).
	FreshnessPossiblyStale Freshness = "possibly_stale"
)

// ErrNotFound marks a cache miss (the Redis adapter maps redis.Nil onto it).
var ErrNotFound = errors.New("cache: key not found")

// ErrFallbackExhausted is returned when the bounded PostgreSQL fallback could
// not start inside the configured timeout: the cache degrades a query, it
// never queues an unbounded stampede against the authority.
var ErrFallbackExhausted = errors.New("cache: bounded fallback exhausted")

// Store is the Redis command subset the cache drives. Implementations live in
// redis.go (production) and in tests (fakes); every method must honor the
// caller's context deadline.
type Store interface {
	// Get returns the stored bytes or ErrNotFound on a miss.
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) (int64, error)
	// Scan walks every key matching match in bounded batches, handing each
	// batch to fn. A non-nil fn error aborts the walk.
	Scan(ctx context.Context, match string, count int64, fn func(keys []string) error) error
	Exists(ctx context.Context, key string) (bool, error)
}

// Observer mirrors cache outcomes into the 013 metric series (T003);
// *metrics.Metrics satisfies it. A nil observer disables observation.
type Observer interface {
	ObserveCacheHit(family string)
	ObserveCacheMiss(family string)
	ObserveCacheFallback(family string)
	ObserveCacheEpochRotation()
}

// Config bounds one cache client. Every field is required and validated
// fail-closed: there is no unbounded default.
type Config struct {
	// Epoch is the initial namespace token (config TXHARBOR_REDIS_CACHE_EPOCH).
	Epoch string
	// TTL is the fallback freshness bound (initial 30s for display models,
	// calibrated after measurement; contracts/redis.md §2.4).
	TTL time.Duration
	// Timeout bounds every Redis round trip and the fallback load.
	Timeout time.Duration
	// MaxFallbackConcurrency bounds concurrent PostgreSQL fallback loads
	// (singleflight + this semaphore + Timeout; contracts/redis.md §2.5).
	// Initial value, calibrated after measurement.
	MaxFallbackConcurrency int
	// Clock is the time source; nil means time.Now.
	Clock func() time.Time
}

// Client is one cache-aside client. It is safe for concurrent use.
type Client struct {
	store    Store
	observer Observer

	epoch atomic.Value // string

	ttl     time.Duration
	timeout time.Duration
	clock   func() time.Time

	sem   chan struct{}
	group *flightGroup

	epochSeq atomic.Uint64
}

// NewClient validates the bounds fail-closed and builds the client.
func NewClient(store Store, cfg Config) (*Client, error) {
	if store == nil {
		return nil, errors.New("cache: store is required")
	}
	if cfg.Epoch == "" {
		return nil, errors.New("cache: epoch is required")
	}
	if cfg.TTL <= 0 {
		return nil, errors.New("cache: TTL must be positive (no unbounded default)")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("cache: timeout must be positive")
	}
	if cfg.MaxFallbackConcurrency <= 0 {
		return nil, errors.New("cache: max fallback concurrency must be positive")
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	c := &Client{
		store:    store,
		observer: nil,
		ttl:      cfg.TTL,
		timeout:  cfg.Timeout,
		clock:    clock,
		sem:      make(chan struct{}, cfg.MaxFallbackConcurrency),
		group:    newFlightGroup(),
	}
	c.epoch.Store(cfg.Epoch)
	return c, nil
}

// SetObserver attaches the metric observer (nil is allowed).
func (c *Client) SetObserver(observer Observer) { c.observer = observer }

// CurrentEpoch returns the active namespace token.
func (c *Client) CurrentEpoch() string {
	epoch, _ := c.epoch.Load().(string)
	return epoch
}

// RotateEpoch advances the namespace: every pre-rotation key becomes
// unreachable (contracts/redis.md §2.2). Rotation is local and cheap; the
// cache is non-authoritative, so concurrent instances may hold different
// epochs without any correctness consequence.
func (c *Client) RotateEpoch() string {
	epoch, _ := c.epoch.Load().(string)
	next := epoch
	if n, err := strconv.ParseUint(epoch, 10, 64); err == nil {
		next = strconv.FormatUint(n+1, 10)
	} else {
		next = fmt.Sprintf("%s-r%d", epoch, c.epochSeq.Add(1))
	}
	c.epoch.Store(next)
	if c.observer != nil {
		c.observer.ObserveCacheEpochRotation()
	}
	return next
}

// Key builds the documented cache key `txharbor:<epoch>:<family>:<id>`.
func (c *Client) Key(family Family, id string) string {
	return "txharbor:" + c.CurrentEpoch() + ":" + string(family) + ":" + id
}

// FamilyPattern is the key pattern of one family under the current epoch.
func (c *Client) FamilyPattern(family Family) string {
	return "txharbor:" + c.CurrentEpoch() + ":" + string(family) + ":*"
}

// sentinelKey is the per-epoch marker that detects a cleared/rebuild Redis.
func (c *Client) sentinelKey() string {
	return "txharbor:" + c.CurrentEpoch() + ":epoch"
}

// EnsureEpoch verifies the epoch sentinel exists; a missing sentinel means the
// namespace was cleared or rebuilt (Redis flush/restart), so the epoch is
// rotated before the new sentinel is written — pre-rotation values stay
// unreachable instead of resurfacing as stale hits (contracts/redis.md §2.2).
// A Redis failure reports false and rotates nothing.
func (c *Client) EnsureEpoch(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	exists, err := c.store.Exists(ctx, c.sentinelKey())
	if err != nil {
		return false
	}
	if exists {
		return true
	}
	c.RotateEpoch()
	// Best effort: a failed write just means the next EnsureEpoch rotates
	// again (rotation is always safe).
	_ = c.store.Set(ctx, c.sentinelKey(), []byte("1"), 0)
	return true
}

// Item is one cache read outcome.
type Item struct {
	// Value is the cached payload bytes.
	Value []byte
	// SourceVersion is the authoritative version the value was derived from
	// (0 when the source did not provide one).
	SourceVersion int64
	// CachedAt is when the value was written into the cache.
	CachedAt time.Time
	// Freshness is the response annotation.
	Freshness Freshness
}

type storedValue struct {
	Value         []byte    `json:"value"`
	SourceVersion int64     `json:"source_version"`
	CachedAt      time.Time `json:"cached_at"`
}

// Get reads one cached value. A miss, an unavailable Redis or an unreadable
// payload returns ok=false: the caller falls back to the authority. A Redis
// failure is counted as a fallback, a clean miss as a miss.
func (c *Client) Get(ctx context.Context, family Family, id string) (Item, bool) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	raw, err := c.store.Get(ctx, c.Key(family, id))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if c.observer != nil {
				c.observer.ObserveCacheMiss(string(family))
			}
		} else if c.observer != nil {
			c.observer.ObserveCacheFallback(string(family))
		}
		return Item{}, false
	}
	var stored storedValue
	if err := json.Unmarshal(raw, &stored); err != nil {
		// An unreadable value is untrusted: never serve it.
		if c.observer != nil {
			c.observer.ObserveCacheFallback(string(family))
		}
		return Item{}, false
	}
	if c.observer != nil {
		c.observer.ObserveCacheHit(string(family))
	}
	return Item{
		Value:         stored.Value,
		SourceVersion: stored.SourceVersion,
		CachedAt:      stored.CachedAt,
		Freshness:     FreshnessFresh,
	}, true
}

// Loader reads the authoritative value directly from PostgreSQL. sourceVersion
// is the authority's own version watermark (0 when unavailable); load MUST
// honor the caller's context.
type Loader func(ctx context.Context) (value []byte, sourceVersion int64, err error)

// GetOrLoad is the cache-aside read:
//
//  1. a cache hit is returned annotated (fresh vs possibly_stale against
//     knownVersion, the caller's authoritative watermark);
//  2. a miss or an unavailable Redis falls back to a direct authoritative
//     load behind the bounded guard (singleflight per key + concurrency
//     semaphore + Timeout) — an outage never stampedes PostgreSQL;
//  3. the loaded value is cached best-effort (a write failure only costs the
//     next reader another authoritative read).
//
// The returned item is always an authoritative load result (FreshnessFresh)
// or an annotated hit; it is never a stale value presented as fresh.
func (c *Client) GetOrLoad(ctx context.Context, family Family, id string, knownVersion int64, load Loader) (Item, error) {
	if !family.Valid() {
		return Item{}, fmt.Errorf("cache: unknown family %q", family)
	}
	if load == nil {
		return Item{}, errors.New("cache: loader is required")
	}
	if item, ok := c.Get(ctx, family, id); ok {
		item.Freshness = annotateFreshness(item, knownVersion)
		return item, nil
	}
	value, err := c.group.Do(c.Key(family, id), func() (any, error) {
		return c.loadBounded(ctx, load)
	})
	if err != nil {
		return Item{}, err
	}
	loaded := value.(Item)
	c.storeBestEffort(ctx, family, id, loaded)
	return loaded, nil
}

// loadBounded runs one authoritative load under the fallback semaphore and
// the configured timeout. Waiting for a slot is itself bounded: an exhausted
// guard degrades the query instead of unboundedly queueing it.
func (c *Client) loadBounded(ctx context.Context, load Loader) (Item, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-waitCtx.Done():
		return Item{}, fmt.Errorf("%w: %v", ErrFallbackExhausted, waitCtx.Err())
	}
	loadCtx, cancelLoad := context.WithTimeout(ctx, c.timeout)
	defer cancelLoad()
	value, sourceVersion, err := load(loadCtx)
	if err != nil {
		return Item{}, err
	}
	return Item{
		Value:         value,
		SourceVersion: sourceVersion,
		CachedAt:      c.clock(),
		Freshness:     FreshnessFresh,
	}, nil
}

// storeBestEffort writes the loaded value with the TTL fallback bound; a
// failure is ignored (never a correctness issue for a non-authoritative
// cache).
func (c *Client) storeBestEffort(ctx context.Context, family Family, id string, item Item) {
	stored, err := json.Marshal(storedValue{
		Value:         item.Value,
		SourceVersion: item.SourceVersion,
		CachedAt:      item.CachedAt,
	})
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_ = c.store.Set(writeCtx, c.Key(family, id), stored, c.ttl)
}

// annotateFreshness applies the contract §2.3 rule: a value is fresh only
// when its source version is confirmable and not behind the known watermark.
func annotateFreshness(item Item, knownVersion int64) Freshness {
	if item.SourceVersion <= 0 {
		return FreshnessPossiblyStale
	}
	if knownVersion > 0 && item.SourceVersion < knownVersion {
		return FreshnessPossiblyStale
	}
	return FreshnessFresh
}

// Invalidate deletes the exact keys of the given family/id pairs (used by the
// event-driven invalidator; deletion is idempotent).
func (c *Client) Invalidate(ctx context.Context, family Family, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		keys = append(keys, c.Key(family, id))
	}
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.store.Del(ctx, keys...)
	return err
}

// InvalidateFamily deletes every key of one family under the current epoch
// (bounded SCAN + batch DEL). Used when an event affects a family-level view.
func (c *Client) InvalidateFamily(ctx context.Context, family Family) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.store.Scan(ctx, c.FamilyPattern(family), 128, func(keys []string) error {
		if len(keys) == 0 {
			return nil
		}
		_, err := c.store.Del(ctx, keys...)
		return err
	})
}

// flightGroup is an in-package singleflight: concurrent loads of the same key
// share one authoritative read. A canceled leader surfaces its error to the
// waiters, who then fall back on their own next attempt (bounded by the
// caller's context).
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{calls: make(map[string]*flightCall)}
}

// Do runs fn once per key at a time; concurrent callers share the result.
func (g *flightGroup) Do(key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if call, ok := g.calls[key]; ok {
		g.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	call := &flightCall{}
	call.wg.Add(1)
	g.calls[key] = call
	g.mu.Unlock()

	call.val, call.err = fn()
	call.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return call.val, call.err
}
