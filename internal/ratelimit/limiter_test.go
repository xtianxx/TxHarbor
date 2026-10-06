// limiter_test.go is the T061 unit evidence: trusted decision parsing, the
// unavailable classification (connection/script error, untrusted result),
// graded reopening after recovery, fail-closed configuration bounds, and the
// structural assertion that limiting never imports an authentication,
// authorization or idempotency package.
package ratelimit

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeScriptStore records evaluations and returns scripted results. A non-nil
// onEval hook replaces the scripted result/error, which lets a test drive the
// evaluation with the store's own context (budget-expiry scenarios).
type fakeScriptStore struct {
	mu     sync.Mutex
	result any
	err    error
	onEval func(ctx context.Context) (any, error)
	calls  []scriptCall
}

type scriptCall struct {
	keys []string
	args []any
}

func (s *fakeScriptStore) Eval(ctx context.Context, _ string, keys []string, args ...any) (any, error) {
	s.mu.Lock()
	s.calls = append(s.calls, scriptCall{keys: append([]string(nil), keys...), args: append([]any(nil), args...)})
	onEval, result, err := s.onEval, s.result, s.err
	s.mu.Unlock()
	if onEval != nil {
		return onEval(ctx)
	}
	return result, err
}

func (s *fakeScriptStore) lastCall(t *testing.T) scriptCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		t.Fatal("no script evaluation recorded")
	}
	return s.calls[len(s.calls)-1]
}

// fakeObserver records metric calls.
type fakeObserver struct {
	mu          sync.Mutex
	denied      []string
	unavailable []bool
	recovered   []string
}

func (o *fakeObserver) ObserveRateLimitDenied(class string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.denied = append(o.denied, class)
}

func (o *fakeObserver) SetRateLimitUnavailable(unavailable bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unavailable = append(o.unavailable, unavailable)
}

func (o *fakeObserver) ObserveRateLimitRecovery(class string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recovered = append(o.recovered, class)
}

func testConfig() Config {
	return Config{
		Classes: map[Class]ClassConfig{
			ClassNewWithdrawal: {RatePerSecond: 10, Burst: 5},
			ClassWrite:         {RatePerSecond: 10, Burst: 5},
			ClassQuery:         {RatePerSecond: 100, Burst: 50},
			ClassOperator:      {RatePerSecond: 5, Burst: 2},
			ClassRPC:           {RatePerSecond: 50, Burst: 20},
		},
		Timeout:        time.Second,
		BucketTTL:      2 * time.Second,
		RecoveryWindow: time.Minute,
	}
}

func TestLimiterTrustedAllowAndDeny(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(4), int64(0)}}
	obs := &fakeObserver{}
	limiter, err := NewLimiter(store, testConfig(), obs)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	decision, err := limiter.Allow(context.Background(), ClassNewWithdrawal)
	if err != nil || !decision.Allowed || decision.Remaining != 4 {
		t.Fatalf("Allow = (%+v, %v), want allowed with 4 remaining", decision, err)
	}
	if call := store.lastCall(t); call.keys[0] != "txharbor:rl:new_withdrawal" {
		t.Fatalf("script key = %q, want txharbor:rl:new_withdrawal", call.keys[0])
	}

	store.result = []any{int64(0), int64(0), int64(250)}
	decision, err = limiter.Allow(context.Background(), ClassNewWithdrawal)
	if err != nil || decision.Allowed {
		t.Fatalf("Allow = (%+v, %v), want denied", decision, err)
	}
	if decision.RetryAfter != 250*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 250ms", decision.RetryAfter)
	}
	if len(obs.denied) != 1 || obs.denied[0] != "new_withdrawal" {
		t.Fatalf("denied observations = %v, want [new_withdrawal]", obs.denied)
	}
}

// TestLimiterBucketTTLIsIndependentOfBudget: the key TTL (ARGV[5]) comes from
// Config.BucketTTL alone; the per-evaluation budget never scales it.
func TestLimiterBucketTTLIsIndependentOfBudget(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	cfg := testConfig()
	cfg.Timeout = 300 * time.Millisecond
	cfg.BucketTTL = 4 * time.Second
	limiter, err := NewLimiter(store, cfg, nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	if _, err := limiter.Allow(context.Background(), ClassRPC); err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if got := store.lastCall(t).args[4]; got != int64(4000) {
		t.Fatalf("script ttl arg = %v, want 4000ms (BucketTTL, not 2×Timeout)", got)
	}
}

func TestLimiterScriptErrorAndUntrustedResultMarkUnavailable(t *testing.T) {
	store := &fakeScriptStore{err: errors.New("connection refused")}
	obs := &fakeObserver{}
	limiter, err := NewLimiter(store, testConfig(), obs)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	if _, err := limiter.Allow(context.Background(), ClassQuery); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Allow during a Redis outage = %v, want ErrUnavailable", err)
	}
	if limiter.Available() {
		t.Fatal("Available() = true after a Redis outage")
	}
	if len(obs.unavailable) != 1 || obs.unavailable[0] != true {
		t.Fatalf("unavailable observations = %v, want [true]", obs.unavailable)
	}

	// An untrusted result (wrong shape) is also unavailable, never an allow.
	store.err = nil
	store.result = "garbage"
	if _, err := limiter.Allow(context.Background(), ClassQuery); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Allow with an untrusted result = %v, want ErrUnavailable", err)
	}
	store.result = []any{int64(2), int64(0), int64(0)}
	if _, err := limiter.Allow(context.Background(), ClassQuery); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Allow with an out-of-range allowed flag = %v, want ErrUnavailable", err)
	}

	// The first trusted decision recovers: gauge cleared and recovery counted.
	store.result = []any{int64(1), int64(1), int64(0)}
	if _, err := limiter.Allow(context.Background(), ClassQuery); err != nil {
		t.Fatalf("Allow after recovery: %v", err)
	}
	if !limiter.Available() {
		t.Fatal("Available() = false after a trusted decision")
	}
	if len(obs.unavailable) != 2 || obs.unavailable[1] != false {
		t.Fatalf("unavailable observations = %v, want a trailing false", obs.unavailable)
	}
	if len(obs.recovered) != 1 || obs.recovered[0] != "query" {
		t.Fatalf("recovery observations = %v, want [query]", obs.recovered)
	}
}

func TestLimiterRecoveryIsGradedNotInstant(t *testing.T) {
	store := &fakeScriptStore{err: errors.New("timeout")}
	obs := &fakeObserver{}
	now := time.Unix(1_700_000_000, 0)
	cfg := testConfig()
	cfg.RecoveryWindow = time.Minute
	cfg.Clock = func() time.Time { return now }
	limiter, err := NewLimiter(store, cfg, obs)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}

	if _, err := limiter.Allow(context.Background(), ClassNewWithdrawal); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("outage Allow = %v, want ErrUnavailable", err)
	}
	// Recover: the first trusted decision runs at half rate/burst.
	store.err = nil
	store.result = []any{int64(1), int64(1), int64(0)}
	if _, err := limiter.Allow(context.Background(), ClassNewWithdrawal); err != nil {
		t.Fatalf("recovery Allow: %v", err)
	}
	call := store.lastCall(t)
	if got := call.args[0]; got != 5 {
		t.Fatalf("recovery rate = %v, want 5 (half of 10)", got)
	}
	if got := call.args[1]; got != 2 {
		t.Fatalf("recovery burst = %v, want 2 (half of 5)", got)
	}
	if !limiter.Recovering() {
		t.Fatal("Recovering() = false inside the recovery window")
	}

	// After the window the full rate/burst return.
	now = now.Add(2 * time.Minute)
	if _, err := limiter.Allow(context.Background(), ClassNewWithdrawal); err != nil {
		t.Fatalf("post-recovery Allow: %v", err)
	}
	call = store.lastCall(t)
	if call.args[0] != 10 || call.args[1] != 5 {
		t.Fatalf("post-recovery rate/burst = (%v, %v), want (10, 5)", call.args[0], call.args[1])
	}
	if limiter.Recovering() {
		t.Fatal("Recovering() = true after the window")
	}
}

// timeoutErr is a net.Error whose Timeout() reports true, standing in for a
// transport-level i/o timeout.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// TestLimiterEvalFailureClassification pins the four-way classification of an
// Eval failure: caller cancellation and a caller deadline never mark the
// limiter, a budget or transport fault always does, and a real transport
// fault is never masked by a coincident cancellation.
func TestLimiterEvalFailureClassification(t *testing.T) {
	cancelled, cancelCancelled := context.WithCancel(context.Background())
	cancelCancelled()
	defer cancelCancelled()
	// A deadline in the past makes the context already expired, so the
	// parent-deadline branch is deterministic.
	expired, cancelExpired := context.WithTimeout(context.Background(), -time.Second)
	defer cancelExpired()

	cases := []struct {
		name         string
		ctx          context.Context
		store        *fakeScriptStore
		wantMarked   bool
		wantCanceled bool
		wantDeadline bool
	}{
		{
			name:         "caller cancel is reported as cancel and never marks",
			ctx:          cancelled,
			store:        &fakeScriptStore{err: context.Canceled},
			wantCanceled: true,
		},
		{
			name:         "caller cancel with a net timeout is still cancel",
			ctx:          cancelled,
			store:        &fakeScriptStore{err: timeoutErr{}},
			wantCanceled: true,
		},
		{
			name:         "caller deadline with a net timeout marks as deadline",
			ctx:          expired,
			store:        &fakeScriptStore{err: timeoutErr{}},
			wantMarked:   true,
			wantDeadline: true,
		},
		{
			name: "budget expiry marks as deadline and never as cancel",
			ctx:  context.Background(),
			store: &fakeScriptStore{onEval: func(ctx context.Context) (any, error) {
				<-ctx.Done()
				return nil, timeoutErr{}
			}},
			wantMarked:   true,
			wantDeadline: true,
		},
		{
			name:       "a transport fault is never masked by a coincident cancel",
			ctx:        cancelled,
			store:      &fakeScriptStore{err: io.EOF},
			wantMarked: true,
		},
		{
			name:       "plain transport error marks",
			ctx:        context.Background(),
			store:      &fakeScriptStore{err: errors.New("connection refused")},
			wantMarked: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Timeout = 50 * time.Millisecond
			limiter, err := NewLimiter(tc.store, cfg, nil)
			if err != nil {
				t.Fatalf("NewLimiter: %v", err)
			}
			if _, err := limiter.Allow(tc.ctx, ClassRPC); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Allow = %v, want an error wrapping ErrUnavailable", err)
			} else if got := errors.Is(err, context.Canceled); got != tc.wantCanceled {
				t.Fatalf("errors.Is(err, context.Canceled) = %v, want %v (err = %v)", got, tc.wantCanceled, err)
			} else if got := errors.Is(err, context.DeadlineExceeded); got != tc.wantDeadline {
				t.Fatalf("errors.Is(err, context.DeadlineExceeded) = %v, want %v (err = %v)", got, tc.wantDeadline, err)
			}
			if got := limiter.Unavailable(); got != tc.wantMarked {
				t.Fatalf("Unavailable() = %v, want %v", got, tc.wantMarked)
			}
		})
	}
}

func TestLimiterUnknownClassFailsClosed(t *testing.T) {
	store := &fakeScriptStore{result: []any{int64(1), int64(1), int64(0)}}
	limiter, err := NewLimiter(store, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	if _, err := limiter.Allow(context.Background(), Class("bogus")); err == nil || errors.Is(err, ErrUnavailable) {
		t.Fatalf("Allow(unconfigured class) = %v, want a fail-closed configuration error", err)
	}
}

func TestNewLimiterRejectsUnboundedConfig(t *testing.T) {
	valid := testConfig()
	cases := []struct {
		name  string
		store ScriptStore
		cfg   Config
	}{
		{"nil store", nil, valid},
		{"zero timeout", &fakeScriptStore{}, Config{Classes: valid.Classes, BucketTTL: time.Second, RecoveryWindow: time.Minute}},
		{"zero bucket ttl", &fakeScriptStore{}, Config{Classes: valid.Classes, Timeout: time.Second, RecoveryWindow: time.Minute}},
		{"zero recovery", &fakeScriptStore{}, Config{Classes: valid.Classes, Timeout: time.Second, BucketTTL: time.Second}},
		{"no classes", &fakeScriptStore{}, Config{Timeout: time.Second, BucketTTL: time.Second, RecoveryWindow: time.Minute}},
		{"unknown class", &fakeScriptStore{}, Config{Classes: map[Class]ClassConfig{"bogus": {1, 1}}, Timeout: time.Second, BucketTTL: time.Second, RecoveryWindow: time.Minute}},
		{"zero rate", &fakeScriptStore{}, Config{Classes: map[Class]ClassConfig{ClassQuery: {0, 1}}, Timeout: time.Second, BucketTTL: time.Second, RecoveryWindow: time.Minute}},
		{"zero burst", &fakeScriptStore{}, Config{Classes: map[Class]ClassConfig{ClassQuery: {1, 0}}, Timeout: time.Second, BucketTTL: time.Second, RecoveryWindow: time.Minute}},
	}
	for _, tc := range cases {
		if _, err := NewLimiter(tc.store, tc.cfg, nil); err == nil {
			t.Errorf("%s: NewLimiter succeeded, want fail-closed error", tc.name)
		}
	}
}

// TestRatelimitNeverImportsFundingPackages is the structural half of
// "limiting is never authentication/authorization/idempotency": production
// sources of this package may not import a funding or identity package.
func TestRatelimitNeverImportsFundingPackages(t *testing.T) {
	forbidden := []string{
		"github.com/xtianxx/txharbor/internal/withdrawal",
		"github.com/xtianxx/txharbor/internal/execution",
		"github.com/xtianxx/txharbor/internal/nonce",
		"github.com/xtianxx/txharbor/internal/txlifecycle",
		"github.com/xtianxx/txharbor/internal/signer",
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	found := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		found++
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			for _, bad := range forbidden {
				if importPath == bad {
					t.Errorf("%s imports %s: limiting must never touch funding/identity packages", name, importPath)
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no production sources found; scan is stale")
	}
}
