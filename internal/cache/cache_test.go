// cache_test.go is the T059 unit evidence: key layout, freshness annotation,
// epoch rotation/unreachability, fail-closed configuration bounds and the
// bounded PostgreSQL fallback (singleflight collapse + semaphore ceiling +
// timeout), all against an in-memory Store double.
package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is an in-memory Store with failure injection and a scan walk.
type fakeStore struct {
	mu     sync.Mutex
	values map[string][]byte
	ttls   map[string]time.Duration

	failGet    error
	failSet    error
	failExists error
	getCalls   atomic.Int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{values: make(map[string][]byte), ttls: make(map[string]time.Duration)}
}

func (s *fakeStore) Get(_ context.Context, key string) ([]byte, error) {
	s.getCalls.Add(1)
	if s.failGet != nil {
		return nil, s.failGet
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), raw...), nil
}

func (s *fakeStore) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if s.failSet != nil {
		return s.failSet
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), value...)
	s.ttls[key] = ttl
	return nil
}

func (s *fakeStore) Del(_ context.Context, keys ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, key := range keys {
		if _, ok := s.values[key]; ok {
			delete(s.values, key)
			delete(s.ttls, key)
			n++
		}
	}
	return n, nil
}

func (s *fakeStore) Scan(_ context.Context, match string, _ int64, fn func(keys []string) error) error {
	prefix := strings.TrimSuffix(match, "*")
	s.mu.Lock()
	var keys []string
	for key := range s.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	s.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}
	return fn(keys)
}

func (s *fakeStore) Exists(_ context.Context, key string) (bool, error) {
	if s.failExists != nil {
		return false, s.failExists
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[key]
	return ok, nil
}

func newTestClient(t *testing.T, store Store, cfg Config) *Client {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	client, err := NewClient(store, cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func TestClientKeyLayout(t *testing.T) {
	client := newTestClient(t, newFakeStore(), Config{
		Epoch: "7", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	if got, want := client.Key(FamilyDeposit, "obs-1"), "txharbor:7:deposit:obs-1"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	if got, want := client.FamilyPattern(FamilyWithdrawal), "txharbor:7:withdrawal:*"; got != want {
		t.Fatalf("FamilyPattern = %q, want %q", got, want)
	}
}

func TestNewClientRejectsUnboundedConfig(t *testing.T) {
	valid := Config{Epoch: "1", TTL: time.Second, Timeout: time.Second, MaxFallbackConcurrency: 1}
	cases := []struct {
		name  string
		store Store
		cfg   Config
	}{
		{"nil store", nil, valid},
		{"empty epoch", newFakeStore(), Config{TTL: time.Second, Timeout: time.Second, MaxFallbackConcurrency: 1}},
		{"zero ttl", newFakeStore(), Config{Epoch: "1", Timeout: time.Second, MaxFallbackConcurrency: 1}},
		{"zero timeout", newFakeStore(), Config{Epoch: "1", TTL: time.Second, MaxFallbackConcurrency: 1}},
		{"zero concurrency", newFakeStore(), Config{Epoch: "1", TTL: time.Second, Timeout: time.Second}},
	}
	for _, tc := range cases {
		if _, err := NewClient(tc.store, tc.cfg); err == nil {
			t.Errorf("%s: NewClient succeeded, want fail-closed error", tc.name)
		}
	}
}

func TestClientGetOrLoadCacheAsideAndFreshness(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 2,
	})
	ctx := context.Background()
	loads := 0
	load := func(context.Context) ([]byte, int64, error) {
		loads++
		return []byte("payload"), 5, nil
	}

	first, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 5, load)
	if err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}
	if loads != 1 || first.Freshness != FreshnessFresh || string(first.Value) != "payload" {
		t.Fatalf("first = %+v loads=%d, want authoritative payload fresh", first, loads)
	}

	second, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 5, load)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if loads != 1 || second.Freshness != FreshnessFresh {
		t.Fatalf("second = %+v loads=%d, want cache hit without a second load", second, loads)
	}

	// A known watermark ahead of the cached version must be labeled
	// possibly_stale (the caller never mistakes it for current authority).
	stale, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 6, load)
	if err != nil {
		t.Fatalf("stale GetOrLoad: %v", err)
	}
	if stale.Freshness != FreshnessPossiblyStale || loads != 1 {
		t.Fatalf("stale = %+v loads=%d, want possibly_stale from cache", stale, loads)
	}
}

func TestClientGetOrLoadUnknownVersionIsPossiblyStale(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	ctx := context.Background()
	loads := 0
	load := func(context.Context) ([]byte, int64, error) {
		loads++
		return []byte("payload"), 0, nil
	}
	// A direct authoritative load is fresh even when the source cannot report
	// a version: it was just read from PostgreSQL.
	first, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 0, load)
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if first.Freshness != FreshnessFresh || loads != 1 {
		t.Fatalf("direct load = %+v loads=%d, want fresh authoritative read", first, loads)
	}
	// A cached hit whose version cannot be confirmed is labeled
	// possibly_stale and never presented as fresh.
	second, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 0, load)
	if err != nil {
		t.Fatalf("GetOrLoad hit: %v", err)
	}
	if second.Freshness != FreshnessPossiblyStale || loads != 1 {
		t.Fatalf("hit = %+v loads=%d, want possibly_stale from cache", second, loads)
	}
}

func TestClientRedisFailureFallsBackDirectly(t *testing.T) {
	store := newFakeStore()
	store.failGet = errors.New("redis: connection refused")
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 2,
	})
	item, err := client.GetOrLoad(context.Background(), FamilyWithdrawal, "w-1", 3, func(context.Context) ([]byte, int64, error) {
		return []byte("authority"), 3, nil
	})
	if err != nil {
		t.Fatalf("GetOrLoad with Redis down: %v", err)
	}
	if string(item.Value) != "authority" || item.Freshness != FreshnessFresh {
		t.Fatalf("item = %+v, want direct authoritative read", item)
	}
	// A cache write failure is best-effort: the read still succeeded.
	store.failSet = errors.New("redis: timeout")
	if _, err := client.GetOrLoad(context.Background(), FamilyWithdrawal, "w-2", 0, func(context.Context) ([]byte, int64, error) {
		return []byte("authority-2"), 1, nil
	}); err != nil {
		t.Fatalf("GetOrLoad with Redis write failure: %v", err)
	}
}

func TestClientFallbackSingleflightCollapsesSameKey(t *testing.T) {
	store := newFakeStore()
	store.failGet = errors.New("redis: down")
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: 5 * time.Second, MaxFallbackConcurrency: 4,
	})
	const callers = 16
	var loads atomic.Int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLoader := func() { releaseOnce.Do(func() { close(release) }) }
	load := func(context.Context) ([]byte, int64, error) {
		loads.Add(1)
		<-release
		return []byte("v"), 1, nil
	}
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			if _, err := client.GetOrLoad(context.Background(), FamilyDeposit, "same", 0, load); err != nil {
				t.Errorf("GetOrLoad: %v", err)
			}
		}()
	}
	// Every failure path must still drain the callers parked in the loader.
	defer func() {
		releaseLoader()
		wg.Wait()
	}()

	// Singleflight merges calls that actually overlap. Release the loader only
	// once the leader is inside it and every other caller is parked in that
	// same in-flight call: the first version released as soon as the first
	// loader entry appeared, so callers that had not reached flightGroup.Do
	// yet started extra loads after the leader returned and deleted the entry
	// — the assertion then failed for scheduling reasons, not a merge defect.
	waitForSingleflightOverlap(t, t.Name(), callers-1, &loads)
	releaseLoader()
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1 (singleflight)", got)
	}
}

// waitForSingleflightOverlap blocks until one loader is running and wantWaiters
// other callers are parked in that same in-flight call; only then may the
// loader be released. flightGroup exposes no test seam for this state, so it
// is observed from goroutine stacks: a caller parked in
// sync.(*WaitGroup).Wait below Client.GetOrLoad is inside flightGroup.Do with
// the call still in flight and cannot leave before the loader is released. It
// fails fast on a second loader (broken merge) and on timeout instead of
// releasing into an unproven window.
func waitForSingleflightOverlap(t *testing.T, testName string, wantWaiters int, loads *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	buf := make([]byte, 4<<20)
	for {
		if got := loads.Load(); got > 1 {
			t.Fatalf("loader calls = %d while the first is still in flight, want 1 (singleflight)", got)
		}
		if n := parkedFlightWaiters(buf, testName); n >= wantWaiters {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d callers to enter the in-flight singleflight call", wantWaiters)
		}
		runtime.Gosched()
	}
}

// parkedFlightWaiters counts goroutines started by testName that are blocked in
// sync.(*WaitGroup).Wait inside Client.GetOrLoad.
func parkedFlightWaiters(buf []byte, testName string) int {
	n := runtime.Stack(buf, true)
	count := 0
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(block, testName) &&
			strings.Contains(block, "sync.(*WaitGroup).Wait") &&
			strings.Contains(block, "cache.(*Client).GetOrLoad") {
			count++
		}
	}
	return count
}

func TestClientFallbackSemaphoreBoundsConcurrency(t *testing.T) {
	store := newFakeStore()
	store.failGet = errors.New("redis: down")
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: 5 * time.Second, MaxFallbackConcurrency: 2,
	})
	var inFlight, maxInFlight atomic.Int64
	release := make(chan struct{})
	load := func(context.Context) ([]byte, int64, error) {
		current := inFlight.Add(1)
		for {
			max := maxInFlight.Load()
			if current <= max || maxInFlight.CompareAndSwap(max, current) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return []byte("v"), 1, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = client.GetOrLoad(context.Background(), FamilyDeposit, fmt.Sprintf("key-%d", i), 0, load)
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for inFlight.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("max concurrent fallback loads = %d, want <= 2 (semaphore)", got)
	}
}

func TestClientFallbackTimeoutIsBounded(t *testing.T) {
	store := newFakeStore()
	store.failGet = errors.New("redis: down")
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: 100 * time.Millisecond, MaxFallbackConcurrency: 1,
	})
	release := make(chan struct{})
	blocking := func(context.Context) ([]byte, int64, error) {
		<-release
		return []byte("v"), 1, nil
	}
	go func() { _, _ = client.GetOrLoad(context.Background(), FamilyDeposit, "blocker", 0, blocking) }()
	// Wait until the blocker holds the only fallback slot.
	time.Sleep(20 * time.Millisecond)
	_, err := client.GetOrLoad(context.Background(), FamilyDeposit, "waiter", 0, func(context.Context) ([]byte, int64, error) {
		return []byte("v"), 1, nil
	})
	close(release)
	if !errors.Is(err, ErrFallbackExhausted) {
		t.Fatalf("GetOrLoad while the guard is exhausted = %v, want ErrFallbackExhausted", err)
	}
}

func TestClientEpochRotationMakesOldKeysUnreachable(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	ctx := context.Background()
	if _, err := client.GetOrLoad(ctx, FamilyDeposit, "obs-1", 1, func(context.Context) ([]byte, int64, error) {
		return []byte("old"), 1, nil
	}); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-1"); !ok {
		t.Fatal("primed value not readable before rotation")
	}

	oldEpoch := client.CurrentEpoch()
	if next := client.RotateEpoch(); next == oldEpoch {
		t.Fatalf("RotateEpoch kept %q", next)
	}
	if _, ok := client.Get(ctx, FamilyDeposit, "obs-1"); ok {
		t.Fatal("pre-rotation value still reachable after epoch rotation")
	}
	// The value physically remains in the store but is unreachable under the
	// new epoch: the cache never serves an invalidated value.
	store.mu.Lock()
	_, present := store.values["txharbor:"+oldEpoch+":deposit:obs-1"]
	store.mu.Unlock()
	if !present {
		t.Fatal("test setup: old value should still exist under the old epoch")
	}
}

func TestClientEnsureEpochRotatesOnClearedNamespace(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	ctx := context.Background()

	// A cleared namespace (sentinel missing) rotates and writes the sentinel.
	epochBefore := client.CurrentEpoch()
	if !client.EnsureEpoch(ctx) {
		t.Fatal("EnsureEpoch = false, want true")
	}
	if client.CurrentEpoch() == epochBefore {
		t.Fatal("EnsureEpoch did not rotate on a cleared namespace")
	}
	// The second call sees the sentinel and does not rotate.
	epochAfter := client.CurrentEpoch()
	if !client.EnsureEpoch(ctx) {
		t.Fatal("second EnsureEpoch = false, want true")
	}
	if client.CurrentEpoch() != epochAfter {
		t.Fatal("EnsureEpoch rotated with an intact sentinel")
	}

	// A Redis failure is not a rotation trigger.
	store.failExists = errors.New("redis: down")
	epoch := client.CurrentEpoch()
	if client.EnsureEpoch(ctx) {
		t.Fatal("EnsureEpoch = true while Redis is unavailable")
	}
	if client.CurrentEpoch() != epoch {
		t.Fatal("EnsureEpoch rotated during a Redis failure")
	}
}

func TestClientInvalidateFamily(t *testing.T) {
	store := newFakeStore()
	client := newTestClient(t, store, Config{
		Epoch: "1", TTL: time.Minute, Timeout: time.Second, MaxFallbackConcurrency: 1,
	})
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := client.store.Set(ctx, client.Key(FamilyDeposit, id), []byte("{}"), time.Minute); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	if err := client.store.Set(ctx, client.Key(FamilyWithdrawal, "keep"), []byte("{}"), time.Minute); err != nil {
		t.Fatalf("seed keep: %v", err)
	}
	if err := client.InvalidateFamily(ctx, FamilyDeposit); err != nil {
		t.Fatalf("InvalidateFamily: %v", err)
	}
	if _, ok := client.Get(ctx, FamilyDeposit, "a"); ok {
		t.Fatal("family invalidation left deposit/a behind")
	}
	if _, ok := client.Get(ctx, FamilyWithdrawal, "keep"); !ok {
		t.Fatal("family invalidation deleted another family")
	}
}
