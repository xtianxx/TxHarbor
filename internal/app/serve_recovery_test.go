package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/health"
	"github.com/xtianxx/txharbor/internal/indexer"
)

// TestServeRefusesBadReorgMaxDepth pins the 006 FR-03/Q1 refusal at the
// process gate (T017a): every illegal TXHARBOR_REORG_MAX_DEPTH aborts Serve
// via the fail() path ("startup failed") before any listener is opened or
// any connection is dialed — no database needed to prove it.
func TestServeRefusesBadReorgMaxDepth(t *testing.T) {
	addr := freePort(t)

	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "missing", mutate: func(m map[string]string) { delete(m, config.EnvReorgMaxDepth) }},
		{name: "empty", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "" }},
		{name: "zero", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "0" }},
		{name: "negative", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "-5" }},
		{name: "non-integer", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "abc" }},
		{name: "beyond int64", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "9223372036854775808" }},
		{name: "beyond uint64", mutate: func(m map[string]string) { m[config.EnvReorgMaxDepth] = "18446744073709551616" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := fullServeEnv(addr)
			tc.mutate(env)

			var stdout, stderr bytes.Buffer
			code := Serve(context.Background(), Deps{
				Getenv:  fakeEnv(env),
				Stdout:  &stdout,
				Stderr:  &stderr,
				Signals: make(chan os.Signal),
			})
			if code == 0 {
				t.Fatalf("Serve() exit code = 0, want non-zero; stderr=%s", stderr.String())
			}
			if !strings.Contains(stderr.String(), config.EnvReorgMaxDepth) {
				t.Fatalf("stderr %q does not name %s", stderr.String(), config.EnvReorgMaxDepth)
			}
			if !strings.Contains(stderr.String(), "startup failed") {
				t.Fatalf("stderr %q is not a startup failure (want the fail() path)", stderr.String())
			}
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("address %s still in use after startup refusal: %v; stderr=%s", addr, err, stderr.String())
			}
			ln.Close()
		})
	}
}

// TestServeRefusesBadReorgReplayBatch pins the replay-cap knob at the config
// gate: a non-positive value names the variable like every other batch knob.
func TestServeRefusesBadReorgReplayBatch(t *testing.T) {
	addr := freePort(t)

	for _, raw := range []string{"0", "abc"} {
		env := fullServeEnv(addr)
		env[config.EnvReorgReplayBatch] = raw

		var stderr bytes.Buffer
		code := Serve(context.Background(), Deps{
			Getenv:  fakeEnv(env),
			Stderr:  &stderr,
			Signals: make(chan os.Signal),
		})
		if code == 0 {
			t.Fatalf("Serve() exit code = 0, want non-zero for replay batch %q", raw)
		}
		if !strings.Contains(stderr.String(), config.EnvReorgReplayBatch) {
			t.Fatalf("stderr %q does not name %s", stderr.String(), config.EnvReorgReplayBatch)
		}
	}
}

// TestBuildRecoveryConfigMapsKnobs pins the serve-to-executor mapping: raw
// depth passthrough, INDEX timing triple reuse (no new knob names), replay
// cap, and the 002 scan start as the executor's pre-start floor.
func TestBuildRecoveryConfigMapsKnobs(t *testing.T) {
	cfg := &config.Config{
		ReorgMaxDepthRaw:  "100",
		IndexPollInterval: 2 * time.Second,
		IndexRetryInitial: 300 * time.Millisecond,
		IndexRetryMax:     10 * time.Second,
		ReorgReplayBatch:  250,
		StartHeight:       7,
	}
	rc := buildRecoveryConfig(cfg, 31337)
	if rc.ChainID != 31337 {
		t.Fatalf("ChainID = %d, want 31337", rc.ChainID)
	}
	if rc.MaxDepthRaw != "100" {
		t.Fatalf("MaxDepthRaw = %q, want %q", rc.MaxDepthRaw, "100")
	}
	if rc.PollInterval != 2*time.Second || rc.RetryInitial != 300*time.Millisecond || rc.RetryMax != 10*time.Second {
		t.Fatalf("timing = (%s,%s,%s), want the INDEX triple", rc.PollInterval, rc.RetryInitial, rc.RetryMax)
	}
	if rc.ReplayHeights != 250 {
		t.Fatalf("ReplayHeights = %d, want 250", rc.ReplayHeights)
	}
	if rc.BlockStartHeight != 7 {
		t.Fatalf("BlockStartHeight = %d, want 7", rc.BlockStartHeight)
	}
}

// fakeStreamLease is a scripted coordinatorLease: tests control acquisition
// wins and heartbeat lifetime without a database.
type fakeStreamLease struct {
	mu             sync.Mutex
	acquires       int
	acquire        func(ctx context.Context, call int) (bool, int64, error)
	heartbeat      func(ctx context.Context) error
	heartbeatCalls int
}

func (f *fakeStreamLease) Acquire(ctx context.Context) (bool, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquires++
	return f.acquire(ctx, f.acquires)
}

func (f *fakeStreamLease) Heartbeat(ctx context.Context) error {
	f.mu.Lock()
	f.heartbeatCalls++
	f.mu.Unlock()
	return f.heartbeat(ctx)
}

func winningLease() *fakeStreamLease {
	return &fakeStreamLease{
		acquire: func(ctx context.Context, call int) (bool, int64, error) {
			return true, int64(call), nil
		},
		heartbeat: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
	}
}

// recordingServe returns a ServeFunc that counts invocations and runs fn.
func recordingServe(calls *atomic.Int64, fn func(ctx context.Context, checkLost func() error) error) indexer.ServeFunc {
	return func(ctx context.Context, checkLost func() error) error {
		calls.Add(1)
		return fn(ctx, checkLost)
	}
}

func nilServe(calls *atomic.Int64) indexer.ServeFunc {
	return recordingServe(calls, func(ctx context.Context, checkLost func() error) error { return nil })
}

// TestRunServiceStreamsStartsAllFiveLoops drives the startup-path wiring
// (runServiceStreams, the exact call serve.go makes) with fakes: the win is
// immediate, every loop — header, log, deposit, confirm, recovery — must be
// invoked, and the coordinator must exit clean.
func TestRunServiceStreamsStartsAllFiveLoops(t *testing.T) {
	lease := winningLease()
	var calls [5]atomic.Int64
	serves := []indexer.ServeFunc{
		nilServe(&calls[0]),
		nilServe(&calls[1]),
		nilServe(&calls[2]),
		nilServe(&calls[3]),
		nilServe(&calls[4]), // recovery: the fifth stream (T017a wiring)
	}

	err := runServiceStreams(context.Background(), lease,
		serves[0], serves[1], serves[2], serves[3], serves[4])
	if err != nil {
		t.Fatalf("runServiceStreams() error = %v, want nil", err)
	}
	names := []string{"header", "log", "deposit", "confirm", "recovery"}
	for i := range calls {
		if got := calls[i].Load(); got != 1 {
			t.Fatalf("%s loop invocations = %d, want 1 (all five loops must start)", names[i], got)
		}
	}
}

// TestRunServiceStreamsCancelExitsClean covers both cancellation polarities
// of the shared loop: a cancelled parent never starts work, and a mid-run
// cancel drains every loop (including recovery) to a clean nil.
func TestRunServiceStreamsCancelExitsClean(t *testing.T) {
	t.Run("pre-cancelled", func(t *testing.T) {
		lease := winningLease()
		var calls [5]atomic.Int64
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := runServiceStreams(ctx, lease,
			nilServe(&calls[0]), nilServe(&calls[1]), nilServe(&calls[2]),
			nilServe(&calls[3]), nilServe(&calls[4]))
		if err != nil {
			t.Fatalf("runServiceStreams() error = %v, want nil on cancellation", err)
		}
		for i := range calls {
			if got := calls[i].Load(); got != 0 {
				t.Fatalf("loop %d invocations = %d, want 0 (cancelled before acquisition)", i, got)
			}
		}
	})

	t.Run("mid-run", func(t *testing.T) {
		lease := winningLease()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{}, 5)
		waitDone := func(ctx context.Context, checkLost func() error) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		}
		var calls [5]atomic.Int64
		done := make(chan error, 1)
		go func() {
			done <- runServiceStreams(ctx, lease,
				recordingServe(&calls[0], waitDone), recordingServe(&calls[1], waitDone),
				recordingServe(&calls[2], waitDone), recordingServe(&calls[3], waitDone),
				recordingServe(&calls[4], waitDone))
		}()
		for range 5 {
			<-started // every loop — including recovery — is running
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("runServiceStreams() error = %v, want nil on mid-run cancel", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("runServiceStreams did not exit after cancellation")
		}
	})
}

// TestRunServiceStreamsLoopErrorPropagates pins the runStreams error
// polarity through the serve wiring: a failing loop's stop error (here the
// recovery loop) reaches the caller so serve exits non-zero.
func TestRunServiceStreamsLoopErrorPropagates(t *testing.T) {
	lease := winningLease()
	boom := errors.New("recovery boom")
	var calls [5]atomic.Int64
	recovery := recordingServe(&calls[4], func(ctx context.Context, checkLost func() error) error { return boom })

	err := runServiceStreams(context.Background(), lease,
		nilServe(&calls[0]), nilServe(&calls[1]), nilServe(&calls[2]),
		nilServe(&calls[3]), recovery)
	if !errors.Is(err, boom) {
		t.Fatalf("runServiceStreams() error = %v, want the failing loop's error", err)
	}
	if got := calls[4].Load(); got != 1 {
		t.Fatalf("recovery loop invocations = %d, want 1", got)
	}
}

// TestRunServiceStreamsLeaseLossReacquires pins the other half of the
// polarity: lease loss is NOT a stop error — the shared loop re-acquires
// and runs all five again instead of exiting.
func TestRunServiceStreamsLeaseLossReacquires(t *testing.T) {
	lease := winningLease()
	var calls [5]atomic.Int64
	// First round the recovery loop reports lease loss; every later round
	// is clean. The coordinator must ride through to a clean nil.
	var recoveryOnce atomic.Int64
	recovery := recordingServe(&calls[4], func(ctx context.Context, checkLost func() error) error {
		if recoveryOnce.Add(1) == 1 {
			return indexer.ErrLeaseLost
		}
		return nil
	})

	err := runServiceStreams(context.Background(), lease,
		nilServe(&calls[0]), nilServe(&calls[1]), nilServe(&calls[2]),
		nilServe(&calls[3]), recovery)
	if err != nil {
		t.Fatalf("runServiceStreams() error = %v, want nil after lease-loss reacquire", err)
	}
	lease.mu.Lock()
	acquires := lease.acquires
	lease.mu.Unlock()
	if acquires < 2 {
		t.Fatalf("acquisitions = %d, want >= 2 (loss must re-acquire, not exit)", acquires)
	}
	if got := calls[4].Load(); got < 2 {
		t.Fatalf("recovery loop invocations = %d, want >= 2 (re-run after reacquire)", got)
	}
}

// TestRecoveryObserverKeepsReadinessSeparate proves the (c) contract with a
// scripted reader: an active recovery annotates recovering/provisional, a
// reconcile-held one annotates paused_reconcile/unknown, and neither flips
// the readiness aggregate — recovery activity, chain pauses and validity
// stay three separate signals.
func TestRecoveryObserverKeepsReadinessSeparate(t *testing.T) {
	agg := health.New("db", "rpc", "version", "chain")
	agg.Set("db", nil)
	agg.Set("rpc", nil)
	agg.Set("version", nil)
	agg.Set("chain", nil)
	if !agg.Ready() {
		t.Fatal("aggregate not ready in the test setup")
	}

	ancestor := int64(90)
	active := &indexer.RecoveryRow{Phase: "detected", AncestorNumber: &ancestor}
	held := &indexer.RecoveryRow{Phase: "reconcile_required", AncestorNumber: &ancestor}

	rows := []*indexer.RecoveryRow{active, held, nil}
	o := &recoveryObserver{read: func(ctx context.Context) (indexer.RecoveryState, indexer.Validity, bool) {
		// Heights above the ancestor are affected-range answers while a
		// recovery row exists (contracts/observability.md validity rules).
		state, validity := indexer.AnnotateRecoveryHeight(rows[0], false, 100)
		rows = rows[1:]
		return state, validity, true
	}}

	ctx := context.Background()
	o.observe(ctx)
	if o.last != indexer.RecoveryStateRecovering || o.lastValidity != indexer.ValidityProvisionalReplaying {
		t.Fatalf("active observe = (%q,%q), want (recovering,provisional_replaying)",
			o.last, o.lastValidity)
	}
	o.observe(ctx)
	if o.last != indexer.RecoveryStatePausedReconcile || o.lastValidity != indexer.ValidityUnknownPaused {
		t.Fatalf("held observe = (%q,%q), want (paused_reconcile,unknown_paused)",
			o.last, o.lastValidity)
	}
	o.observe(ctx)
	if o.last != indexer.RecoveryStateNone || o.lastValidity != indexer.ValidityUnaffected {
		t.Fatalf("idle observe = (%q,%q), want (none,valid_unaffected)", o.last, o.lastValidity)
	}
	if !agg.Ready() {
		t.Fatal("readiness flipped during recovery observation: recovery state must never drive readyz")
	}

	// A failed read keeps the last observed state instead of inventing one.
	stuck := &recoveryObserver{
		read:         func(ctx context.Context) (indexer.RecoveryState, indexer.Validity, bool) { return "", "", false },
		last:         o.last,
		lastValidity: o.lastValidity,
		sampled:      true,
	}
	stuck.observe(ctx)
	if stuck.last != indexer.RecoveryStateNone || stuck.lastValidity != indexer.ValidityUnaffected {
		t.Fatalf("failed read moved state to (%q,%q), want the last observed pair kept",
			stuck.last, stuck.lastValidity)
	}
}
