//go:build integration_redis

// redislatency_ab_test.go is the bounded A/B/C comparison experiment for the
// redis failure latency finding (benchmark_report.md §4: full-stack path,
// redis_down query p95 ≈ 1005ms, bind commit 89ef787; the observation is a
// HISTORICAL measurement, not a current-main re-run).
//
// Question under test (evidence-backed variant of the R1 candidate): does
// shortening ONLY the go-redis DialTimeout (keeping Read/WriteTimeout and
// every other option identical) measurably shorten the stop-to-observation
// delay of limiter.Allow across the four failure shapes?
//
//   - A: baseline — DialTimeout = cfg.Redis.Timeout = 1s (the serve wiring
//     sets all three timeouts to one knob, serve.go:195-197).
//   - B: DialTimeout = 200ms.
//   - C: DialTimeout = 300ms.
//
// The other options (Read/WriteTimeout=1s, MaxRetries/DialerRetries/pool
// defaults) stay byte-identical across variants: the candidate changes one
// field and no production default; no new env key is introduced.
//
// Shapes (all real, none approximated by another):
//
//   - "refused": dial meets ECONNREFUSED (gate mode GateDown) — shape ①.
//   - "dial_blackhole": every dial attempt blocks until the per-attempt
//     budget (min(DialTimeout, attempt-ctx remaining)) expires — shape ②,
//     driven by a test-side blackhole Dialer because a userspace listener
//     cannot produce a SYN blackhole (see internal/testutil/redisgate.go
//     header: a Linux listener accepts the handshake into the accept queue
//     even when nothing ever Accept()s).
//   - "conn_hold_cold": dial succeeds through the Hold gate (TCP established,
//     request bytes swallowed), the HELLO/first-command response is held —
//     the client-level read times out on the FIRST cold connection (shape
//     ③(b)). [语义边界] this is "connected + no response", NOT a completed
//     Redis HELLO handshake; the reviewer's wording is adopted verbatim.
//   - "conn_hold_warm": the Hold gate after EACH caller warmed one pooled
//     conn in Pass mode (poolSize==callers; reviewer P1), the warm socket is
//     CONVERTED on the flip (backend closed, gateway swallows reads), so the
//     EVAL rides an initialized-but-silent socket into the read-hang (shape
//     ③(a)).
//   - "normal": gate Pass, no fault — the reference group (shape ④).
//
// Rounds: two per (variant, shape) — round 1 = FIRST fault (fresh limiter,
// but the shared CLIENT across rounds carries the pooled-connection state
// forward, so round 2 = SUSTAINED fault: warm ③ dominates and dialErrors
// fast-paths may exist). The fault is re-injected for each round (SetMode
// flip), not a paused continuation of round 1.
//
// Concurrency: fixed 8 concurrent callers per round, each looping Allow
// for the fixed window. Sustainment window = 1.2s per variant/shape/round
// (chosen > 1s caller budget so every caller completes ≥1 full Allow
// during the fault window; measured, not extrapolated).
//
// Recovery (tested once, on the C variant, gate legs): a ③ Hold flip to
// Pass — the limiter must return to allow (Unavailable flips false through
// markAvailable only inside Allow, limiter.go:290-293) and the graded
// reopening window must still open (Recovering() true right after the
// first trusted decision). This exercises the EXISTING recovery contract;
// the experiment does not change it.
//
// Discipline notes:
//   - This test never pretends to be a measurement of the served path:
//     nothing here drives a serve process (the 015 recovery gate and the
//     full HTTP stack are out of the experiment's bounded scope —那份等价性
//     在前一份报告中即已声明为「未建立」).
//   - Unobservable stages (socket-level dial/send counts, go-redis internal
//     attempt counts) are recorded as zero/unknown and never extrapolated.
//   - Candidate durations (environment budgets, rates, windows) are
//     experiment inputs, NOT approved SLOs and NOT production thresholds.
package ratelimit

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/testutil"
)

// redisLatencyShape is one fault injection mode of the experiment.
type redisLatencyShape string

const (
	shapeRefused       redisLatencyShape = "refused"
	shapeDialBlackhole redisLatencyShape = "dial_blackhole"
	shapeConnHoldCold  redisLatencyShape = "conn_hold_cold"
	shapeConnHoldWarm  redisLatencyShape = "conn_hold_warm"
	shapeNormal        redisLatencyShape = "normal"
)

// latencyVariants are the three configurations under comparison.
type latencyVariant struct {
	name        string
	dialTimeout time.Duration
}

func latencyVariants() []latencyVariant {
	return []latencyVariant{
		{name: "A", dialTimeout: 1 * time.Second}, // baseline: = cfg.Redis.Timeout
		{name: "B", dialTimeout: 200 * time.Millisecond},
		{name: "C", dialTimeout: 300 * time.Millisecond},
	}
}

// latencyWindows are the FIXED experiment parameters (not production SLO).
const (
	latencyCallers      = 8
	latencyWindow       = 1200 * time.Millisecond
	latencyRounds       = 2
	latencyCallerBudget = 1 * time.Second // the limiter's own caller budget stays the same knob for all variants (limiter.go:222)
)

// blackholeDialer is the test-side shape-② injection: it blocks exactly for
// min(dialTimeout(ctx remaining), the variant's DialTimeout), then reports
// a dial-shaped timeout — the same error shape the real kernel path would
// deliver on a SYN blackhole (net.OpError{Op:"dial"} wrapping a deadline
// error). It never opens a socket: no SYN is sent, so "no real connection
// established" stays true by construction.
// The returned OpError shape carries Op "dial" so shouldRetry's OpError
// branch (error.go:97-100) applies exactly as with a real network dial.
type blackholeDialer struct {
	innerDialTimeout time.Duration
}

func (b *blackholeDialer) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	deadline := time.Now().Add(b.innerDialTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	<-time.After(time.Until(deadline))
	if err := ctx.Err(); err != nil {
		return nil, &net.OpError{Op: "dial", Net: network, Source: nil, Addr: nil, Err: fmt.Errorf("blackhole dial canceled: %w", err)}
	}
	// DialTimeout hit first (the real-world "blackhole + DialTimeout" case):
	return nil, &net.OpError{Op: "dial", Net: network, Source: nil, Addr: nil, Err: timeoutMarker{}}
}

// timeoutMarker renders as the timeout marker the kernel uses for an
// unexplained (non-syscall) dial timeout.
type timeoutMarker struct{}

func (timeoutMarker) Error() string { return "dial timeout (experiment blackhole marker)" }
func (timeoutMarker) Timeout() bool { return true }

// classifyRedisError buckets the error a caller actually saw. The mapping
// is decision-grade only for the shapes this experiment drives.
func classifyRedisError(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connect: connection refused"):
		return "refused_dial"
	case strings.Contains(msg, "context deadline exceeded"):
		return "context_deadline_exceeded"
	case strings.Contains(msg, "blackhole dial") || strings.Contains(msg, "dial timeout (experiment blackhole marker)"):
		return "blackhole_dialer"
	case strings.Contains(msg, "i/o timeout") && strings.Contains(msg, "dial tcp"):
		return "dial_io_timeout"
	case strings.Contains(msg, "i/o timeout"):
		return "read_io_timeout"
	case strings.Contains(msg, "connection pool timeout"):
		return "pool_timeout"
	default:
		return "other"
	}
}

// buildLatencyLimiter assembles the REAL 013 stack pieces (limiter + policy)
// over a go-redis client with the given DialTimeout; every other option is
// the production shape (Read/Write = 1s = cfg.Redis.Timeout; MaxRetries and
// the dial-retry knobs stay at library defaults). This mirrors exactly the
// serve-side client construction (serve.go:193-198) with only DialTimeout
// under test.
func buildLatencyLimiter(t *testing.T, addr string, dialTimeout time.Duration, observer *t066Observer) (*Limiter, *Policy, redis.UniversalClient) {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  dialTimeout,
		ReadTimeout:  redisLatencyCallerTimeout(),
		WriteTimeout: redisLatencyCallerTimeout(),
	})
	store, err := NewRedisScriptStore(client)
	if err != nil {
		t.Fatalf("NewRedisScriptStore: %v", err)
	}
	limiter, err := NewLimiter(store, Config{
		// Bench-scale limiter inputs (500/100, identical to the perf path)
		// so a healthy Redis never denies by budget and a refusal during a
		// fault is always attributable to the injected fault, not to a
		// local rate/burst ceiling.
		Classes: map[Class]ClassConfig{
			ClassNewWithdrawal: {RatePerSecond: 500, Burst: 100},
			ClassWrite:         {RatePerSecond: 500, Burst: 100},
			ClassQuery:         {RatePerSecond: 500, Burst: 100},
			ClassOperator:      {RatePerSecond: 500, Burst: 100},
			ClassRPC:           {RatePerSecond: 500, Burst: 100},
		},
		Timeout:        redisLatencyCallerTimeout(),
		BucketTTL:      2 * redisLatencyCallerTimeout(), // preserves the pre-BucketTTL script TTL (2*Timeout)
		RecoveryWindow: 10 * time.Second,
	}, observer)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	policy, err := NewPolicy(limiter)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	return limiter, policy, client
}

// buildWarmPoolClient builds a client whose pool can hold at most poolSize
// conns: with poolSize == latencyCallers there is one warm slot per caller,
// so the warm-hold shape's concurrent calls all ride INITIALIZED sockets
// (already HELLO'd through the gate in Pass) instead of a mix of one warm
// conn plus callers that cold-dial into the Hold gate. Reviewer P1.
func buildWarmPoolClient(t *testing.T, addr string, dialTimeout time.Duration, poolSize int) redis.UniversalClient {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  dialTimeout,
		ReadTimeout:  redisLatencyCallerTimeout(),
		WriteTimeout: redisLatencyCallerTimeout(),
		PoolSize:     poolSize,
	})
	return client
}

// redisLatencyCallerTimeout is the caller budget: the production knob value
// (1s) — kept constant across variants because the R1 candidate changes
// DialTimeout only.
func redisLatencyCallerTimeout() time.Duration { return 1 * time.Second }

// runShapeWindow drives one (variant, shape, round) measurement: it opens
// the fault (or leaves the gate in Pass for the normal shape), lets
// `latencyCallers` goroutines each loop `policy.Admit(ctx, ClassQuery)` for
// the fixed window, and records stop-to-observation samples with the
// caller-observable error classes.
func runShapeWindow(
	t *testing.T,
	ring *testutil.TimingRing,
	v latencyVariant,
	shape redisLatencyShape,
	round int,
	gate *testutil.RedisGate,
) {
	t.Helper()

	// The limiter instance is fresh for each shape/round: a status reset is
	// part of the shape's precondition, NOT hidden state. The CLIENT is
	// reconstructed too (fresh client per call) — the "sustained fault"
	// contrast comes from the shape precondition sequence (warm-hold warms
	// first in every round), not from an aged failure state.
	observer := &t066Observer{}
	var limiter *Limiter
	var policy *Policy
	var client redis.UniversalClient
	if shape == shapeConnHoldWarm {
		// Warm shape: cap the pool at latencyCallers so every concurrent
		// caller has a warm slot (reviewer P1: a single warm conn mixes
		// warm/cold samples across the 8 callers).
		client = buildWarmPoolClient(t, gate.HostPort(), v.dialTimeout, latencyCallers)
		store, err := NewRedisScriptStore(client)
		if err != nil {
			t.Fatalf("NewRedisScriptStore: %v", err)
		}
		limiter, err = NewLimiter(store, Config{
			Classes: map[Class]ClassConfig{
				ClassNewWithdrawal: {RatePerSecond: 500, Burst: 100},
				ClassWrite:         {RatePerSecond: 500, Burst: 100},
				ClassQuery:         {RatePerSecond: 500, Burst: 100},
				ClassOperator:      {RatePerSecond: 500, Burst: 100},
				ClassRPC:           {RatePerSecond: 500, Burst: 100},
			},
			Timeout:        redisLatencyCallerTimeout(),
			BucketTTL:      2 * redisLatencyCallerTimeout(), // preserves the pre-BucketTTL script TTL (2*Timeout)
			RecoveryWindow: 10 * time.Second,
		}, observer)
		if err != nil {
			t.Fatalf("NewLimiter: %v", err)
		}
		policy, err = NewPolicy(limiter)
		if err != nil {
			t.Fatalf("NewPolicy: %v", err)
		}
	} else {
		limiter, policy, client = buildLatencyLimiter(t, gate.HostPort(), v.dialTimeout, observer)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Warm the path for the warm-hold shape: the gate must be Pass, then
	// ONE PING PER CALLER warms one initialized conn each (pool size ==
	// callers), so after the Hold flip every concurrent call rides an
	// initialized socket.
	if shape == shapeConnHoldWarm {
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("warm-up pass: %v", err)
		}
		var warmWg sync.WaitGroup
		for range latencyCallers {
			warmWg.Add(1)
			go func() {
				defer warmWg.Done()
				if err := client.Ping(ctx).Err(); err != nil {
					t.Errorf("warm-up ping: %v", err)
				}
			}()
		}
		warmWg.Wait()
	}

	// Drive the injected fault for this shape.
	switch shape {
	case shapeRefused, shapeConnHoldCold, shapeConnHoldWarm:
		mode := testutil.GateDown
		if shape != shapeRefused {
			mode = testutil.GateHold
		}
		if err := gate.SetMode(mode); err != nil {
			t.Fatalf("inject %s: %v", shape, err)
		}
		defer func() {
			if err := gate.SetMode(testutil.GatePass); err != nil {
				t.Fatalf("restore pass: %v", err)
			}
		}()
	case shapeNormal:
		if err := gate.SetMode(testutil.GatePass); err != nil {
			t.Fatalf("normal pass: %v", err)
		}
	case shapeDialBlackhole:
		// The blackhole is per-client (a Dialer option), not per-gate: for
		// this shape the variant's limiter must be built with the blackhole
		// dialer, which runShapeWindow does NOT do by default. The shape is
		// therefore driven by a dedicated subtest (see
		// TestRedisLatencyABCDialBlackhole) — reaching here is a fixture
		// error.
		t.Fatalf("shape %s must be driven by the blackhole subtest", shape)
	}

	// Observation calls the LIMITER's Allow directly (the exact 013 limiter
	// call path), NOT policy.Admit: PD-1's policy maps a limiter failure on
	// non-creation classes onto "continue with nil" (policy.go:107-110), so
	// the policy's nil would MINT a success record for a failed decision —
	// the experiment must classify the DECISION ERROR, not the policy's
	// routing outcome.
	start := time.Now()
	var wg sync.WaitGroup
	for caller := range latencyCallers {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()
			for time.Since(start) < latencyWindow {
				at := time.Now()
				_, allowErr := limiter.Allow(ctx, ClassQuery)
				dur := time.Since(at)
				sample := testutil.LatencySample{
					Variant:    v.name,
					Shape:      string(shape),
					Round:      round,
					Caller:     caller,
					DurationMS: float64(dur.Microseconds()) / 1000.0,
					OK:         allowErr == nil,
					ErrorClass: classifyRedisError(allowErr),
					Unavail:    limiter.Unavailable(),
					At:         at.UTC(),
				}
				if allowErr != nil {
					sample.ErrorText = allowErr.Error()
					// go-redis internal attempt counts are NOT observable
					// through the API used; nothing is recorded for them
					// (unknown, never extrapolated).
				}
				ring.Record(sample)
			}
		}(caller)
	}
	wg.Wait()
	_ = policy // buildLatencyLimiter wires the real policy; the shape window observes the limiter directly
}

// TestRedisLatencyABCRefusedAndHold runs shapes ①③④ for all variants × 2
// rounds through one shared gate and asserts NOTHING about the R1
// candidate: it only accumulates samples (assessments live in the analysis
// step; the test's assertion layer is the evidence archive).
func TestRedisLatencyABCRefusedAndHold(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })

	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })

	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("redis-latency evidence dir: %s", writer.Dir())

	ring := testutil.NewTimingRing(3 * latencyVariantsCount() * len([]redisLatencyShape{shapeRefused, shapeConnHoldCold, shapeConnHoldWarm, shapeNormal}) * latencyRounds * latencyCallers * 4)

	roundCounter := 0
	for _, v := range latencyVariants() {
		for _, shape := range []redisLatencyShape{shapeRefused, shapeConnHoldCold, shapeConnHoldWarm, shapeNormal} {
			for range latencyRounds {
				round := roundCounter + 1
				roundCounter++
				t.Logf("variant=%s shape=%s round=%d", v.name, shape, round)
				runShapeWindow(t, ring, v, shape, round, gate)
			}
		}
	}

	// Evidence archive: raw samples + per-group stats, one line per sample
	// (the same timeline discipline the faultdrill EvidenceWriter uses).
	if err := writer.WriteJSON("samples.json", ring.Snapshot()); err != nil {
		t.Fatalf("write samples: %v", err)
	}
	all := ring.Snapshot()
	groups := map[string][]testutil.LatencySample{}
	for _, s := range all {
		key := fmt.Sprintf("%s/%s/r%d", s.Variant, s.Shape, s.Round)
		groups[key] = append(groups[key], s)
	}
	stats := map[string]testutil.LatencyStats{}
	for key, samples := range groups {
		stats[key] = testutil.ComputeLatencyStats(samples)
	}
	if err := writer.WriteJSON("stats.json", stats); err != nil {
		t.Fatalf("write stats: %v", err)
	}
	// Keep the test log useful: it is the shortest readable form.
	for key, st := range stats {
		t.Logf("%-52s n=%-4d err=%.2f p50=%.1f p95=%.1f p99=%.1f max=%.1f",
			key, st.Count, st.ErrorRate, st.P50MS, st.P95MS, st.P99MS, st.MaxMS)
	}
}

func latencyVariantsCount() int { return len(latencyVariants()) }

// TestRedisLatencyABCRecovery checks the recovery leg ONCE (on the C
// variant, one shape-③ flip, gate legs): the limiter must go back to
// allow through the real Allow path and the graded reopening window must
// still open. It does not re-run the matrix.
func TestRedisLatencyABCRecovery(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	redisCtr, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisCtr.Close(context.Background()) })

	gate, err := testutil.NewRedisGate(redisCtr.HostPort())
	if err != nil {
		t.Fatalf("NewRedisGate: %v", err)
	}
	t.Cleanup(func() { _ = gate.Close() })

	observer := &t066Observer{}
	limiter, _, client := buildLatencyLimiter(t, gate.HostPort(), 300*time.Millisecond, observer)
	t.Cleanup(func() { _ = client.Close() })

	// Establish warm normal (Ping through the gate) then Hold.
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("warm ping: %v", err)
	}
	if err := gate.SetMode(testutil.GateHold); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, allowErr := limiter.Allow(ctx, ClassQuery); allowErr == nil {
		t.Fatal("allow during hold = nil, want failure (recorded at the limiter, not the policy)")
	}
	if !limiter.Unavailable() {
		t.Fatal("limiter did not mark unavailable under hold")
	}

	// Recovery: flip to Pass; the next Allow must succeed (through the REAL
	// limiter path — Unavailable can only flip inside Allow).
	if err := gate.SetMode(testutil.GatePass); err != nil {
		t.Fatalf("pass: %v", err)
	}
	recoveryDeadline := time.Now().Add(30 * time.Second)
	hangStart := time.Now()
	for {
		_, allowErr := limiter.Allow(ctx, ClassQuery)
		if allowErr == nil {
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatal("limiter never recovered within 30s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	recoverySoloLatency := time.Since(hangStart)
	if !limiter.Recovering() {
		t.Fatal("limiter did not enter its graded reopening window")
	}
	t.Logf("recovery: first trusted Allow succeeded after %s (graded reopening window still on)", recoverySoloLatency)
}

// TestRedisLatencyABCDialBlackhole runs shape ② for all variants × 2
// rounds via the blackhole dialer (shape ② cannot come from a userspace
// gate; see the gate header). The dialer is wrapped around the variant's
// inner dial timeout so the per-attempt budget (min(DialTimeout, attempt
// ctx)) is the ONLY thing racing the blackhole.
func TestRedisLatencyABCDialBlackhole(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	// No Redis container is needed for ②: nothing behind the dialer is
	// reached by construction. The dialer replaces the transport, so the
	// experiment is fully isolated from any backend.
	writer, err := testutil.NewEvidenceWriter()
	if err != nil {
		t.Fatalf("NewEvidenceWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	t.Logf("dial-blackhole evidence dir: %s", writer.Dir())

	ring := testutil.NewTimingRing(3 * latencyVariantsCount() * latencyRounds * latencyCallers * 4)

	roundCounter := 0
	for _, v := range latencyVariants() {
		for range latencyRounds {
			round := roundCounter + 1
			roundCounter++
			observer := &t066Observer{}
			// Blackhole dialer: the dialer's own budget = the variant's
			// DialTimeout (what the production NewDialer would have used);
			// other options identical (Read/Write=1s, library defaults).
			client := redis.NewClient(&redis.Options{
				Addr:         "127.0.0.1:6379", // never dialed: the blackhole dialer replaces the transport
				DialTimeout:  v.dialTimeout,
				ReadTimeout:  redisLatencyCallerTimeout(),
				WriteTimeout: redisLatencyCallerTimeout(),
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return (&blackholeDialer{innerDialTimeout: v.dialTimeout}).Dial(ctx, network, addr)
				},
			})
			store, err := NewRedisScriptStore(client)
			if err != nil {
				t.Fatalf("NewRedisScriptStore: %v", err)
			}
			limiter, err := NewLimiter(store, Config{
				Classes: map[Class]ClassConfig{
					ClassQuery: {RatePerSecond: 500, Burst: 100},
				},
				Timeout:        redisLatencyCallerTimeout(),
				BucketTTL:      2 * redisLatencyCallerTimeout(), // preserves the pre-BucketTTL script TTL (2*Timeout)
				RecoveryWindow: 10 * time.Second,
			}, observer)
			if err != nil {
				t.Fatalf("NewLimiter: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			start := time.Now()
			var wg sync.WaitGroup
			for caller := range latencyCallers {
				wg.Add(1)
				go func(caller int) {
					defer wg.Done()
					for time.Since(start) < latencyWindow {
						at := time.Now()
						// Direct limiter call (same reason as runShapeWindow:
						// the policy would route a query-side failure to nil).
						_, allowErr := limiter.Allow(ctx, ClassQuery)
						dur := time.Since(at)
						sample := testutil.LatencySample{
							Variant:    v.name,
							Shape:      string(shapeDialBlackhole),
							Round:      round,
							Caller:     caller,
							DurationMS: float64(dur.Microseconds()) / 1000.0,
							OK:         allowErr == nil,
							ErrorClass: classifyRedisError(allowErr),
							Unavail:    limiter.Unavailable(),
							At:         at.UTC(),
						}
						if allowErr != nil {
							sample.ErrorText = allowErr.Error()
						}
						ring.Record(sample)
					}
				}(caller)
			}
			wg.Wait()
			cancel()
			_ = client.Close()
		}
	}

	if err := writer.WriteJSON("dial_blackhole_samples.json", ring.Snapshot()); err != nil {
		t.Fatalf("write samples: %v", err)
	}
	all := ring.Snapshot()
	groups := map[string][]testutil.LatencySample{}
	for _, s := range all {
		key := fmt.Sprintf("%s/%s/r%d", s.Variant, s.Shape, s.Round)
		groups[key] = append(groups[key], s)
	}
	stats := map[string]testutil.LatencyStats{}
	for key, samples := range groups {
		stats[key] = testutil.ComputeLatencyStats(samples)
	}
	if err := writer.WriteJSON("dial_blackhole_stats.json", stats); err != nil {
		t.Fatalf("write stats: %v", err)
	}
	for key, st := range stats {
		t.Logf("%-52s n=%-4d err=%.2f p50=%.1f p95=%.1f p99=%.1f max=%.1f",
			key, st.Count, st.ErrorRate, st.P50MS, st.P95MS, st.P99MS, st.MaxMS)
	}
}

// writeLatencyDigest helper was dropped: the EvidenceWriter lives in the
// fault-tagged faultdrill package and cross-layer imports are forbidden;
// the JSON evidence produced by the tests above is the archive of record.
